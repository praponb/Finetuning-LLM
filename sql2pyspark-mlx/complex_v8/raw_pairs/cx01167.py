from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_latest = Window.partitionBy("status").orderBy(F.col("premium").desc(), F.col("id").desc())
w_all_by_premium = Window.orderBy("premium", "id")
w_all_by_premium_ties = Window.orderBy("premium")
w_status_holder_id = Window.partitionBy("status", "holder_id")
w_all_by_premium_running = Window.orderBy("premium", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

cleaned = (
    df_policies
    .select(
        "id",
        "holder_id",
        "premium",
        "start_date",
        "end_date",
        "status",
    )
)

status_stats = (
    cleaned
    .groupBy("status")
    .agg(F.percentile_approx("premium", 0.5).alias("median_premium"))
)

outliers = (
    cleaned.alias("s")
    .join(status_stats.alias("g"), F.col("g.status") == F.col("s.status"), "inner")
    .filter(F.col("s.premium") >= F.col("g.median_premium"))
    .select(
        "s.*",
        "g.median_premium",
        F.round(F.col("s.premium") - F.col("g.median_premium"), 2).alias("premium_minus_median"),
    )
)

unique_rows = (
    outliers
    .withColumn("row_num", F.row_number().over(w_latest))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

screened = (
    unique_rows.alias("s")
    .join(df_policies.alias("p"), (F.col("p.holder_id") == F.col("s.holder_id")) & (F.col("p.start_date") >= F.lit("2024-11-20").cast("date")) & (F.col("p.end_date") >= F.lit("2026-01-23").cast("date")), "left_anti")
)

windowed = (
    screened
    .select(
        "id",
        "holder_id",
        "premium",
        "end_date",
        "premium_minus_median",
        F.round(F.cume_dist().over(w_all_by_premium), 3).alias("premium_cume_dist"),
        F.rank().over(w_all_by_premium_ties).alias("premium_rank"),
        F.count(F.lit(1)).over(w_status_holder_id).alias("rows_in_status_holder_id"),
        (F.col("premium") - F.lag("premium").over(w_all_by_premium)).alias("premium_change"),
        F.sum("premium").over(w_all_by_premium_running).alias("running_premium"),
    )
)

result = (
    windowed
    .select(
        "id",
        "premium",
        "end_date",
        "premium_minus_median",
        "premium_cume_dist",
        "premium_rank",
        "rows_in_status_holder_id",
        "premium_change",
        "running_premium",
        F.least(F.col("rows_in_status_holder_id"), F.col("premium_rank")).alias("least_rows_in_status_holder_id_premium_rank"),
        F.year(F.col("end_date")).alias("end_date_year"),
    )
    .orderBy(F.col("end_date").desc())
)
