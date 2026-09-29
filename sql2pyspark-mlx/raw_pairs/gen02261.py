from pyspark.sql import functions as F

enriched = (
    df_order_items
    .alias("src")
    .join(df_orders.alias("o"), F.col("src.order_id") == F.col("o.id"), "left")
    .filter(F.col("o.customer_id").isNotNull())
    .select("src.*", F.col("o.order_date"), F.col("o.discount"), F.col("o.channel"))
)

kept_keys = (
    enriched
    .select("order_id", "product_id")
    .subtract(
        df_order_items
        .filter(F.col("product_id") == 18)
        .select("order_id", "product_id")
    )
)

reconciled = (
    enriched
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.order_id") == F.col("k.order_id")) & (F.col("s.product_id") == F.col("k.product_id")), "inner")
    .select("s.*")
)

cleaned = (
    reconciled
    .filter(F.col("product_id").isNotNull())
    .select(
        "id",
        "order_id",
        "product_id",
        "quantity",
        "unit_price",
        "discount",
        F.floor("unit_price").alias("unit_price_whole"),
        F.coalesce("channel", F.lit("N/A")).alias("channel_clean"),
    )
)

with_dims = (
    cleaned
    .alias("src")
    .join(df_products.alias("b"), F.col("src.product_id").eqNullSafe(F.col("b.id")), "inner")
    .select(
        "src.order_id",
        "src.product_id",
        "src.unit_price",
        "src.unit_price_whole",
        "src.channel_clean",
        F.col("b.product_name"),
    )
)

result = (
    with_dims
    .groupBy(
        F.when(F.col("unit_price_whole") >= 72, "HIGH").otherwise("LOW").alias("unit_price_whole_tier"),
        "order_id",
    )
    .agg(
        F.countDistinct("order_id").alias("distinct_order_ids"),
        F.count("*").alias("row_count"),
        F.count("*").alias("row_count_2"),
        F.percentile_approx("unit_price_whole", 0.9).alias("unit_price_whole_p90"),
    )
)
