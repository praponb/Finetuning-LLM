from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id", "store_id").orderBy("quantity", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("store_id").orderBy("quantity", "id")
w3 = Window.orderBy(F.col("first_known_days_since_last_restock").desc(), "id")
w4 = Window.partitionBy("store_id").orderBy("next_days_since_last_restock", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.orderBy("next_days_since_last_restock")
w6 = Window.partitionBy("product_id").orderBy("first_known_days_since_last_restock", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w7 = Window.partitionBy("product_id", "store_id")
w8 = Window.partitionBy("store_id").orderBy("next_days_since_last_restock", "id").rowsBetween(-3, Window.currentRow)
w9 = Window.partitionBy("store_id").orderBy("product_id_stddev_samp_first_known_days_since_last_restock", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w10 = Window.partitionBy("store_id").orderBy("last4_sum_next_days_since_last_restock", "id")

filtered = (
    df_inventory
    .filter(F.col("quantity") >= 170)
    .select(
        "id",
        "store_id",
        "product_id",
        "quantity",
        F.datediff(F.current_date(), "last_restock").alias("days_since_last_restock"),
    )
)

enriched = (
    filtered
    .select(
        "id",
        "store_id",
        "product_id",
        F.first("days_since_last_restock", ignorenulls=True).over(w).alias("first_known_days_since_last_restock"),
        F.lead("days_since_last_restock", 1, 0).over(w2).alias("next_days_since_last_restock"),
    )
)

enriched_inventory = (
    enriched
    .select(
        "id",
        "store_id",
        "next_days_since_last_restock",
        F.ntile(10).over(w3).alias("first_known_days_since_last_restock_bucket"),
        F.sum("first_known_days_since_last_restock").over(w4).alias("running_first_known_days_since_last_restock"),
        F.dense_rank().over(w5).alias("next_days_since_last_restock_dense_rank"),
        F.sum("next_days_since_last_restock").over(w6).alias("cumulative_next_days_since_last_restock"),
        F.stddev_samp("first_known_days_since_last_restock").over(w7).alias("product_id_stddev_samp_first_known_days_since_last_restock"),
    )
)

enriched_inventory_2 = (
    enriched_inventory
    .select(
        "id",
        "store_id",
        "next_days_since_last_restock_dense_rank",
        "cumulative_next_days_since_last_restock",
        "product_id_stddev_samp_first_known_days_since_last_restock",
        F.avg("next_days_since_last_restock").over(w8).alias("last4_avg_next_days_since_last_restock"),
        F.sum("next_days_since_last_restock").over(w8).alias("last4_sum_next_days_since_last_restock"),
        F.min("next_days_since_last_restock").over(w8).alias("last4_min_next_days_since_last_restock"),
    )
)

enriched_inventory_3 = (
    enriched_inventory_2
    .filter(F.col("last4_min_next_days_since_last_restock") >= 36)
    .select(
        "*",
        F.avg("product_id_stddev_samp_first_known_days_since_last_restock").over(w9).alias("running_avg_product_id_stddev_samp_first_known_days_since_last_restock"),
        F.max("product_id_stddev_samp_first_known_days_since_last_restock").over(w9).alias("running_max_product_id_stddev_samp_first_known_days_since_last_restock"),
        F.min("product_id_stddev_samp_first_known_days_since_last_restock").over(w9).alias("running_min_product_id_stddev_samp_first_known_days_since_last_restock"),
        F.count("product_id_stddev_samp_first_known_days_since_last_restock").over(w9).alias("running_count_product_id_stddev_samp_first_known_days_since_last_restock"),
        F.ntile(4).over(w10).alias("last4_sum_next_days_since_last_restock_bucket"),
    )
)

summary = (
    enriched_inventory_3
    .groupBy("store_id")
    .agg(
        F.max("running_max_product_id_stddev_samp_first_known_days_since_last_restock").alias("max_running_max_product_id_stddev_samp_first_known_days_since_last_restock"),
        F.round(F.avg("last4_min_next_days_since_last_restock"), 2).alias("avg_last4_min_next_days_since_last_restock"),
        F.percentile_approx("running_avg_product_id_stddev_samp_first_known_days_since_last_restock", [0.25, 0.5, 0.75]).alias("running_avg_product_id_stddev_samp_first_known_days_since_last_restock_quartiles"),
        F.round(F.avg("last4_avg_next_days_since_last_restock"), 2).alias("avg_last4_avg_next_days_since_last_restock"),
        F.percentile_approx("last4_sum_next_days_since_last_restock", 0.95).alias("last4_sum_next_days_since_last_restock_p95"),
        F.percentile_approx("running_min_product_id_stddev_samp_first_known_days_since_last_restock", 0.95).alias("running_min_product_id_stddev_samp_first_known_days_since_last_restock_p95"),
    )
    .filter(F.col("avg_last4_min_next_days_since_last_restock") >= 0)
)

result = (
    summary
    .select(
        "store_id",
        "avg_last4_min_next_days_since_last_restock",
        "avg_last4_avg_next_days_since_last_restock",
        "last4_sum_next_days_since_last_restock_p95",
        "running_min_product_id_stddev_samp_first_known_days_since_last_restock_p95",
    )
    .orderBy(F.col("last4_sum_next_days_since_last_restock_p95").desc())
)
