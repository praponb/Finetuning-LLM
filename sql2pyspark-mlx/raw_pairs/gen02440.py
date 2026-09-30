from pyspark.sql import functions as F

matched_keys = (
    df_order_items
    .select("order_id", "product_id")
    .subtract(
        df_order_items
        .filter(F.col("product_id").isNotNull())
        .select("order_id", "product_id")
    )
)

verified = (
    df_order_items
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.order_id") == F.col("k.order_id")) & (F.col("s.product_id") == F.col("k.product_id")), "inner")
    .select("s.*")
)

result = (
    verified
    .withColumn("unit_price_tier", F.when(F.col("unit_price") >= 300, "HIGH").otherwise("LOW"))
    .groupingSets(
        [["unit_price_tier", "product_id"], ["unit_price_tier"], []],
        "unit_price_tier",
        "product_id",
    )
    .agg(
        F.count("*").alias("event_count"),
        F.count("*").alias("num_rows"),
        F.round(F.avg("quantity"), 2).alias("avg_quantity"),
        F.percentile_approx("unit_price", [0.25, 0.5, 0.75]).alias("unit_price_quartiles"),
        F.round(F.avg("quantity"), 2).alias("avg_quantity_2"),
        F.percentile_approx("quantity", [0.25, 0.5, 0.75]).alias("quantity_quartiles"),
    )
    .orderBy("avg_quantity_2")
)
