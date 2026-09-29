from pyspark.sql import functions as F

valid = (
    df_order_items
    .filter(F.col("order_id").isin([1, 30, 40]))
    .select("*", F.round((F.col("quantity") * 1.15), 2).alias("quantity_scaled"))
)

kept_keys = (
    valid
    .select("order_id")
    .exceptAll(
        df_order_items
        .filter(F.col("product_id").isin([17, 28]))
        .select("order_id")
    )
)

reconciled = (
    valid
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.order_id") == F.col("k.order_id")), "inner")
    .select("s.*")
)

enriched = (
    reconciled
    .alias("s")
    .join(df_orders.alias("b"), F.col("s.order_id") == F.col("b.id"), "inner")
    .filter(F.col("b.customer_id") == 14)
    .select(
        "s.order_id",
        "s.product_id",
        "s.quantity",
        "s.unit_price",
        F.col("b.amount"),
    )
)

enriched_order_items = (
    enriched
    .alias("s")
    .join(df_products.alias("b"), F.col("s.product_id") == F.col("b.id"), "left")
    .select("s.*", F.col("b.price"), F.col("b.launch_date"))
)

result = (
    enriched_order_items
    .withColumn("week_start", F.date_trunc("WEEK", "launch_date"))
    .cube("week_start", "product_id")
    .agg(
        F.percentile_approx("price", 0.5).alias("price_p50"),
        F.countDistinct("product_id").alias("distinct_product_ids"),
        F.percentile_approx("quantity", [0.25, 0.5, 0.75]).alias("quantity_quartiles"),
        F.percentile_approx("unit_price", 0.9).alias("unit_price_p90"),
        F.bit_and("quantity").alias("quantity_bit_and"),
        F.corr("quantity", "unit_price").alias("corr_quantity_unit_price"),
        F.grouping_id().alias("grouping_level"),
    )
    .orderBy(F.col("product_id").desc_nulls_last())
)
