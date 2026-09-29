from pyspark.sql import functions as F

valid = (
    df_order_items
    .filter(F.col("product_id").isNotNull() & (F.col("quantity") > 7))
    .withColumn("unit_price_scaled", F.round((F.col("unit_price") * 0.9), 2))
    .withColumn("quantity_tier", F.when(F.col("quantity") >= 9, "High").when(F.col("quantity") >= 3, "Medium").otherwise("Low"))
)

remaining_keys = (
    valid
    .select("id")
    .subtract(
        df_order_items
        .filter(F.col("order_id") == 6)
        .select("id")
    )
)

verified = (
    valid
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

matched_keys = (
    verified
    .select("product_id", "id")
    .intersect(
        df_order_items
        .filter(F.col("unit_price") >= 230)
        .select("product_id", "id")
    )
)

kept_rows = (
    verified
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.product_id") == F.col("k.product_id")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

with_dims = (
    kept_rows
    .alias("src")
    .join(df_products.alias("d"), F.col("src.product_id") == F.col("d.id"), "left")
    .select(
        "src.id",
        "src.order_id",
        "src.product_id",
        "src.quantity",
        F.col("d.brand"),
        F.col("d.category"),
    )
)

result = (
    with_dims
    .alias("src")
    .join(df_orders.alias("p"), F.col("src.order_id") == F.col("p.id"), "inner")
    .groupBy(F.col("p.channel").alias("channel"), F.col("src.brand").alias("brand"))
    .agg(
        F.count("*").alias("cnt"),
        F.min(F.col("src.quantity")).alias("min_quantity"),
    )
)
