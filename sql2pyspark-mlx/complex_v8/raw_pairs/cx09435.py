from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_status = Window.partitionBy("status")
w_holder_id_end_date_month_by = Window.partitionBy("holder_id", F.month("end_date")).orderBy("end_date")
w_holder_id_end_date_month_by_2 = Window.partitionBy("holder_id", F.month("end_date")).orderBy("end_date", "id")
w_status_by_start_date_running = Window.partitionBy("status").orderBy(F.col("start_date").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_status_by_start_date_full = Window.partitionBy("status").orderBy(F.col("start_date").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_status_by_premium_ties = Window.partitionBy("status").orderBy("premium")
w_end_date_dense_rank = Window.partitionBy("end_date_dense_rank")
w_end_date_dense_rank_by_rows = Window.partitionBy("end_date_dense_rank").orderBy("rows_in_status")

filtered = (
    df_policies
    .filter((F.col("premium") < 4670.9) & (F.year(F.col("end_date")) == 2026))
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "start_date",
        "end_date",
        "status",
        F.date_format(F.col("end_date"), "yyyy-MM").alias("end_date_label"),
        F.substring(F.trim(F.col("product")), 1, 2).alias("product_prefix"),
    )
)

with_trends = (
    filtered
    .select(
        "*",
        F.count(F.lit(1)).over(w_status).alias("rows_in_status"),
        F.round(F.avg("premium").over(w_status), 2).alias("status_avg_premium"),
        F.dense_rank().over(w_holder_id_end_date_month_by).alias("end_date_dense_rank"),
        F.round(F.col("premium") - F.avg("premium").over(w_status), 2).alias("premium_vs_status_avg"),
        F.ntile(3).over(w_holder_id_end_date_month_by_2).alias("end_date_tercile"),
        F.sum("premium").over(w_status_by_start_date_running).alias("running_premium"),
        F.nth_value("end_date", 2).over(w_status_by_start_date_full).alias("second_end_date"),
        F.dense_rank().over(w_status_by_premium_ties).alias("premium_dense_rank"),
    )
)

top_per_group = with_trends.filter(F.col("end_date_dense_rank") == 1)

windowed = (
    top_per_group
    .select(
        "*",
        F.sum("premium_dense_rank").over(w_end_date_dense_rank).alias("end_date_dense_rank_sum_premium_dense_rank"),
        F.dense_rank().over(w_end_date_dense_rank_by_rows).alias("rows_in_status_dense_rank"),
        F.round(F.col("end_date_dense_rank") - F.avg("end_date_dense_rank").over(w_end_date_dense_rank), 2).alias("end_date_dense_rank_vs_end_date_dense_rank_avg"),
        F.round(F.col("premium_vs_status_avg") / F.nullif(F.sum("premium_vs_status_avg").over(w_end_date_dense_rank), F.lit(0)), 4).alias("premium_vs_status_avg_share_of_end_date_dense_ra"),
    )
)

grouped = (
    windowed
    .groupBy(
        F.when(F.col("rows_in_status") >= 15, "large").when(F.col("rows_in_status") >= 6, "medium").otherwise("small").alias("rows_in_status_tier"),
        "end_date_dense_rank",
    )
    .agg(
        F.sum("premium_dense_rank").alias("total_premium_dense_rank"),
        F.countDistinct("product").alias("distinct_products"),
        F.sum("running_premium").alias("sum_running_premium_check"),
    )
    .filter(F.col("sum_running_premium_check") > 12650.0)
    .drop("sum_running_premium_check")
)

result = (
    grouped
    .filter(F.col("end_date_dense_rank") < 7)
    .select(
        "rows_in_status_tier",
        "end_date_dense_rank",
        "distinct_products",
        (F.floor(F.col("end_date_dense_rank") / 2) * 2).alias("end_date_dense_rank_bucket"),
        F.substring(F.trim(F.col("rows_in_status_tier")), 1, 4).alias("rows_in_status_tier_prefix"),
    )
    .orderBy(
        F.col("rows_in_status_tier_prefix").desc(),
        F.col("rows_in_status_tier").asc(),
    )
)
