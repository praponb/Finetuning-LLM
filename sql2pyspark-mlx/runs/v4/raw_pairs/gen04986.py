from pyspark.sql import functions as F

joined = (
    df_order_items
    .alias("s")
    .join(df_orders.alias("b"), F.col("s.order_id") == F.col("b.id"), "inner")
    .select("s.*", F.col("b.status"), F.col("b.discount"))
)

result = (
    joined
    .withColumn("unit_price_tier", F.when(F.col("unit_price") >= 400, "HIGH").otherwise("LOW"))
    .rollup("product_id", "unit_price_tier")
    .agg(
        F.round(F.sum("unit_price") / F.nullif(F.count(F.when(F.col("status") == "CANCELLED", 1)), F.lit(0)), 2).alias("unit_price_per_cancelled"),
        F.count("*").alias("event_count"),
        F.grouping("product_id").alias("gid"),
    )
    .filter(F.col("unit_price_per_cancelled") > 0)
)
