from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_status = Window.partitionBy("status")
w_status_by_premium_minus_avg = Window.partitionBy("status").orderBy(F.col("premium_minus_avg").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_status_by_premium_minus_avg_2 = Window.partitionBy("status").orderBy(F.col("premium_minus_avg").desc(), "id")
w_latest = Window.partitionBy("holder_id").orderBy(F.col("premium_minus_avg").desc(), F.col("id").desc())
w_start_date_month_holder_id = Window.partitionBy(F.month("start_date"), "holder_id")
w_start_date_year_by_last_prem = Window.partitionBy(F.year("start_date")).orderBy("last_premium_minus_avg", "id")
w_holder_id_by_premium_minus_a = Window.partitionBy("holder_id").orderBy(F.col("premium_minus_avg_percent_rank").desc(), "id")
w_row_count_first_start_date_y = Window.partitionBy("row_count", F.year("first_start_date")).orderBy(F.col("row_count").desc(), "start_date_month", "holder_id")
w_row_count_first_start_date_y_2 = Window.partitionBy("row_count", F.year("first_start_date")).orderBy(F.col("row_count").desc())
w_first_start_date_month = Window.partitionBy(F.date_trunc("month", "first_start_date"))

scoped_policys = (
    df_policies
    .filter((F.col("premium") >= 1042.5) | (F.col("start_date") < F.lit("2025-02-14").cast("date")))
    .select(
        "id",
        "holder_id",
        "premium",
        "start_date",
        "status",
        F.concat_ws(": ", F.col("product"), F.col("status")).alias("product_status_label"),
        F.initcap(F.trim(F.col("product"))).alias("product_clean"),
    )
)

status_stats = (
    scoped_policys
    .groupBy("status")
    .agg(F.avg("premium").alias("avg_premium"))
)

vs_group = (
    scoped_policys.alias("s")
    .join(status_stats.alias("g"), F.col("g.status") == F.col("s.status"), "inner")
    .filter(F.col("s.premium") > F.col("g.avg_premium"))
    .select(
        "s.*",
        "g.avg_premium",
        F.round(F.col("s.premium") - F.col("g.avg_premium"), 2).alias("premium_minus_avg"),
    )
)

metrics = (
    vs_group
    .select(
        "id",
        "holder_id",
        "start_date",
        "product_status_label",
        "product_clean",
        "premium_minus_avg",
        F.count(F.lit(1)).over(w_status).alias("rows_in_status"),
        F.last("premium_minus_avg").over(w_status_by_premium_minus_avg).alias("last_premium_minus_avg"),
        F.round(F.col("premium") / F.nullif(F.sum("premium").over(w_status), F.lit(0)), 4).alias("premium_share_of_status"),
        F.round(F.percent_rank().over(w_status_by_premium_minus_avg_2), 3).alias("premium_minus_avg_percent_rank"),
    )
)

latest = (
    metrics
    .withColumn("recency_rank", F.row_number().over(w_latest))
    .filter(F.col("recency_rank") == 1)
    .drop("recency_rank")
)

with_trends = (
    latest
    .select(
        "*",
        F.count(F.lit(1)).over(w_start_date_month_holder_id).alias("rows_in_start_date_month_holder_id"),
        F.max("premium_minus_avg").over(w_start_date_month_holder_id).alias("start_date_month_holder_id_max_premium_minus_avg"),
        F.lag("premium_share_of_status").over(w_start_date_year_by_last_prem).alias("prev_premium_share_of_status"),
        F.datediff(F.col("start_date"), F.lag("start_date").over(w_holder_id_by_premium_minus_a)).alias("days_since_prev_start_date"),
    )
)

rollup_base = (
    with_trends
    .groupBy(
        F.date_trunc("month", "start_date").alias("start_date_month"),
        "holder_id",
    )
    .agg(
        F.count("*").alias("row_count"),
        F.min("start_date").alias("first_start_date"),
    )
    .filter(F.col("row_count") >= 2)
)

sequenced = (
    rollup_base
    .select(
        "start_date_month",
        "holder_id",
        "first_start_date",
        F.ntile(5).over(w_row_count_first_start_date_y).alias("row_count_quintile"),
        F.round(F.percent_rank().over(w_row_count_first_start_date_y_2), 3).alias("row_count_percent_rank"),
        F.round(F.avg("row_count").over(w_first_start_date_month), 2).alias("first_start_date_month_avg_row_count"),
    )
)

result = (
    sequenced
    .filter(F.col("row_count_quintile") < 8)
    .select(
        "row_count_percent_rank",
        "first_start_date_month_avg_row_count",
        "start_date_month",
    )
    .distinct()
    .orderBy(
        F.col("first_start_date_month_avg_row_count").asc(),
        F.col("start_date_month").desc_nulls_first(),
    )
)
