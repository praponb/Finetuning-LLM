from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_product_holder_id = Window.partitionBy("product", "holder_id")
w_holder_id_by_premium = Window.partitionBy("holder_id").orderBy("premium", "id")
w_all_by_end_date_ties = Window.orderBy(F.col("end_date").desc())
w_holder_id_by_premium_2 = Window.partitionBy("holder_id").orderBy(F.col("premium").desc(), "id")
w_holder_id_by_premium_running = Window.partitionBy("holder_id").orderBy(F.col("premium").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_holder_id_by_premium_full = Window.partitionBy("holder_id").orderBy(F.col("premium").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_end_date_moving_6 = Window.orderBy(F.col("end_date").desc(), "id").rowsBetween(-6, Window.currentRow)

base_policys = (
    df_policies
    .filter((F.col("product").isNotNull()) & (F.col("start_date") >= F.lit("2024-10-22").cast("date")))
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "start_date",
        "end_date",
        "status",
        F.round(F.col("premium") * 0.9, 2).alias("premium_adj"),
        F.substring(F.trim(F.col("status")), 1, 3).alias("status_prefix"),
    )
)

with_windows = (
    base_policys
    .select(
        "*",
        F.size(F.collect_set("status").over(w_product_holder_id)).alias("distinct_status_in_product_holder_id"),
        F.ntile(3).over(w_holder_id_by_premium).alias("premium_tercile"),
        F.dense_rank().over(w_all_by_end_date_ties).alias("end_date_dense_rank"),
        F.rank().over(w_holder_id_by_premium).alias("premium_rank"),
        (F.col("premium_adj") - F.lag("premium_adj").over(w_holder_id_by_premium_2)).alias("premium_adj_change"),
        F.lag("start_date", 2).over(w_holder_id_by_premium).alias("prev2_start_date"),
        F.max("premium").over(w_holder_id_by_premium_running).alias("max_premium_so_far"),
        F.nth_value("end_date", 2).over(w_holder_id_by_premium_full).alias("second_end_date"),
        F.max("premium_adj").over(w_all_by_end_date_moving_6).alias("premium_adj_max_7"),
        F.round(F.col("premium") - F.avg("premium").over(w_product_holder_id), 2).alias("premium_vs_product_holder_id_avg"),
        F.round(F.avg("premium").over(w_product_holder_id), 2).alias("product_holder_id_avg_premium"),
        F.round(F.col("premium_adj") / F.nullif(F.sum("premium_adj").over(w_product_holder_id), F.lit(0)), 4).alias("premium_adj_share_of_product_holder_id"),
    )
)

totals = (
    with_windows
    .groupBy(
        F.year("prev2_start_date").alias("prev2_start_date_year"),
        F.date_trunc("month", "end_date").alias("end_date_month"),
    )
    .agg(
        F.sum(F.when(F.col("product") != "AUTO", F.col("premium"))).alias("premium_product_auto"),
        F.round(F.sum("max_premium_so_far"), 2).alias("total_max_premium_so_far"),
        F.round(F.stddev("premium_adj_max_7"), 2).alias("stddev_premium_adj_max_7"),
    )
)

result = (
    totals
    .select(
        "prev2_start_date_year",
        "end_date_month",
        "premium_product_auto",
        "total_max_premium_so_far",
        "stddev_premium_adj_max_7",
        (F.col("prev2_start_date_year") - F.col("stddev_premium_adj_max_7")).alias("prev2_start_date_year_minus_stddev_premium_adj_m"),
    )
    .orderBy(
        F.col("prev2_start_date_year").desc(),
        "premium_product_auto",
        "end_date_month",
    )
    .limit(10)
)
