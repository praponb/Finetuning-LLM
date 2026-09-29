from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy("line_total", "id").rowsBetween(-2, -1)
w2 = Window.partitionBy("product_id").orderBy("line_total", "id")
w3 = Window.orderBy("line_total", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("order_id").orderBy("unit_price", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("product_id", "order_id").orderBy("next_line_total", "id").rowsBetween(-2, -1)
w6 = Window.partitionBy("product_id").orderBy(F.col("cumulative_line_total").desc(), "id")
w7 = Window.orderBy("cumulative_unit_price_scaled", "id")
w8 = Window.partitionBy("product_id").orderBy("cumulative_line_total_bucket", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w9 = Window.partitionBy("product_id").orderBy("cumulative_unit_price_scaled")

valid = (
    df_order_items
    .filter(F.col("product_id").isNotNull())
    .select(
        "id",
        "order_id",
        "product_id",
        "unit_price",
        F.round((F.col("unit_price") * 0.9), 2).alias("unit_price_scaled"),
        F.round((F.col("unit_price") / F.col("quantity")), 2).alias("line_total"),
    )
)

with_windows = (
    valid
    .filter(F.col("order_id").isin([15, 16, 36]))
    .select(
        "id",
        "order_id",
        "product_id",
        "line_total",
        F.avg("unit_price_scaled").over(w).alias("unit_price_scaled_avg_2_prior"),
        F.lead("line_total", 2).over(w2).alias("next_line_total"),
        F.sum("unit_price_scaled").over(w3).alias("cumulative_unit_price_scaled"),
        F.sum("line_total").over(w4).alias("cumulative_line_total"),
    )
)

enriched = (
    with_windows
    .alias("s")
    .join(df_products.alias("b"), F.col("s.product_id") == F.col("b.id"), "inner")
    .select("s.*", F.col("b.stock_qty"))
)

with_windows_order_items = (
    enriched
    .select(
        "id",
        "product_id",
        "unit_price_scaled_avg_2_prior",
        "cumulative_unit_price_scaled",
        F.avg("cumulative_line_total").over(w5).alias("cumulative_line_total_avg_2_prior"),
        F.ntile(10).over(w6).alias("cumulative_line_total_bucket"),
        F.lag("next_line_total", 1, 0).over(w7).alias("prev_next_line_total"),
    )
)

metrics = (
    with_windows_order_items
    .select(
        "id",
        "cumulative_unit_price_scaled",
        "cumulative_line_total_avg_2_prior",
        "prev_next_line_total",
        F.sum("unit_price_scaled_avg_2_prior").over(w8).alias("running_unit_price_scaled_avg_2_prior"),
        F.dense_rank().over(w9).alias("cumulative_unit_price_scaled_dense_rank"),
    )
)

result = (
    metrics
    .withColumn("running_unit_price_scaled_avg_2_prior_tier", F.when(F.col("running_unit_price_scaled_avg_2_prior") >= 389, "HIGH").otherwise("LOW"))
    .withColumn("cumulative_line_total_avg_2_prior_tier", F.when(F.col("cumulative_line_total_avg_2_prior") >= 415, "HIGH").otherwise("LOW"))
    .withColumn("prev_next_line_total_tier", F.when(F.col("prev_next_line_total") >= 79, "HIGH").otherwise("LOW"))
    .groupingSets(
        [["running_unit_price_scaled_avg_2_prior_tier", "cumulative_line_total_avg_2_prior_tier", "prev_next_line_total_tier"], ["running_unit_price_scaled_avg_2_prior_tier", "cumulative_line_total_avg_2_prior_tier"], []],
        "running_unit_price_scaled_avg_2_prior_tier",
        "cumulative_line_total_avg_2_prior_tier",
        "prev_next_line_total_tier",
    )
    .agg(
        F.percentile_approx("cumulative_unit_price_scaled_dense_rank", 0.95).alias("cumulative_unit_price_scaled_dense_rank_p95"),
        F.percentile_approx("cumulative_unit_price_scaled_dense_rank", [0.25, 0.5, 0.75]).alias("cumulative_unit_price_scaled_dense_rank_quartiles"),
        F.countDistinct("id").alias("distinct_ids"),
        F.countDistinct("id").alias("distinct_ids_2"),
        F.grouping_id().alias("grouping_level"),
    )
    .filter(F.col("distinct_ids_2") < 1000)
    .orderBy(F.col("running_unit_price_scaled_avg_2_prior_tier").desc())
)
