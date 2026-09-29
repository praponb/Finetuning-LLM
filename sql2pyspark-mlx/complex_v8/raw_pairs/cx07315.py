from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_avg_qty_full = Window.orderBy("avg_qty", "sku_id", "warehouse_id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_avg_qty_prior_2 = Window.orderBy("avg_qty", "sku_id", "warehouse_id").rowsBetween(-2, -1)

filtered_moves = (
    df_stock_moves
    .filter((F.col("move_type") == "TRANSFER") & (F.col("unit_price") > 124.1))
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "qty",
        "moved_at",
        "unit_price",
        F.substring(F.trim(F.col("operator")), 1, 2).alias("operator_prefix"),
        F.when(F.col("unit_price") >= 290.4, "A").when(F.col("unit_price") >= 238.5, "B").otherwise("C").alias("unit_price_tier"),
    )
)

per_group = (
    filtered_moves
    .groupBy("sku_id", "warehouse_id")
    .agg(
        F.max("moved_at").alias("last_moved_at"),
        F.count("*").alias("cnt"),
        F.round(F.stddev("qty"), 2).alias("stddev_qty"),
        F.percentile_approx("qty", 0.5).alias("median_qty"),
        F.round(F.avg("qty"), 2).alias("avg_qty"),
    )
)

metrics = (
    per_group
    .withColumn("first_stddev_qty", F.first("stddev_qty").over(w_all_by_avg_qty_full))
    .withColumn("median_qty_sum_2_prior", F.sum("median_qty").over(w_all_by_avg_qty_prior_2))
)

result = (
    metrics
    .cube("warehouse_id", "cnt")
    .agg(
        F.grouping_id().alias("grouping_level"),
        F.min("last_moved_at").alias("first_last_moved_at"),
        F.round(F.avg("avg_qty"), 2).alias("avg_avg_qty"),
        F.round(F.stddev("stddev_qty"), 2).alias("stddev_stddev_qty"),
    )
)
