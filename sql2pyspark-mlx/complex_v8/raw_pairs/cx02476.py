from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_status_group = Window.partitionBy("status_group")
w_all_by_start_date = Window.orderBy(F.col("start_date").desc(), "id")
w_all_by_start_date_prior_1 = Window.orderBy(F.col("start_date").desc(), "id").rowsBetween(-1, -1)
w_all_by_start_date_running = Window.orderBy(F.col("start_date").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_row_count_rn = Window.partitionBy("row_count", "rn")
w_rn_row_count_by_row_count = Window.partitionBy("rn", "row_count").orderBy(F.col("row_count").asc_nulls_last(), "end_date_ym", "rn", "start_date_ym")

cleaned = (
    df_policies
    .filter((F.col("product").isNotNull()) & (F.col("holder_id") <= 17))
    .select(
        "id",
        "holder_id",
        "premium",
        "start_date",
        "end_date",
        F.when(F.col("status").isin("EXPIRED", "LAPSED"), "group_a").when(F.col("status") == "ACTIVE", "group_b").otherwise("other").alias("status_group"),
        F.upper(F.trim(F.col("product"))).alias("product_clean"),
    )
)

ranked = (
    cleaned
    .select(
        "id",
        "holder_id",
        "premium",
        "start_date",
        "end_date",
        "product_clean",
        F.count(F.lit(1)).over(w_status_group).alias("rows_in_status_group"),
        F.row_number().over(w_all_by_start_date).alias("rn"),
        F.round(F.avg("premium").over(w_all_by_start_date_prior_1), 2).alias("premium_avg_1_prior"),
        F.size(F.collect_set("holder_id").over(w_status_group)).alias("distinct_holder_id_in_status_group"),
        F.first("end_date", ignorenulls=True).over(w_all_by_start_date_running).alias("first_known_end_date"),
    )
)

stats = (
    ranked
    .groupBy(
        F.date_format("end_date", "yyyy-MM").alias("end_date_ym"),
        "rn",
        F.date_format("start_date", "yyyy-MM").alias("start_date_ym"),
    )
    .agg(
        F.round(F.stddev("premium_avg_1_prior"), 2).alias("stddev_premium_avg_1_prior"),
        F.count("*").alias("row_count"),
    )
)

windowed = (
    stats
    .select(
        "end_date_ym",
        "rn",
        "start_date_ym",
        "stddev_premium_avg_1_prior",
        "row_count",
        F.count(F.lit(1)).over(w_row_count_rn).alias("rows_in_row_count_rn"),
        F.max("rn").over(w_row_count_rn).alias("row_count_rn_max_rn"),
        F.row_number().over(w_rn_row_count_by_row_count).alias("rn_2"),
    )
)

result = (
    windowed
    .select(
        "end_date_ym",
        "rn",
        "start_date_ym",
        "stddev_premium_avg_1_prior",
        "row_count",
        "row_count_rn_max_rn",
        "rn_2",
        F.round(F.col("row_count") / F.nullif(F.col("rn_2"), F.lit(0)), 3).alias("row_count_per_rn_2"),
        F.concat(F.col("start_date_ym"), F.lit("#"), F.col("rows_in_row_count_rn").cast("string")).alias("start_date_ym_ref"),
    )
    .orderBy(
        F.col("row_count_per_rn_2").desc(),
        F.col("stddev_premium_avg_1_prior").desc(),
    )
)
