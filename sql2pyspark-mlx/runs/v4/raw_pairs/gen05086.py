from pyspark.sql import functions as F

matched_keys = (
    df_order_items
    .select("id", "product_id")
    .intersect(
        df_order_items
        .filter(F.col("unit_price") > 130)
        .select("id", "product_id")
    )
)

kept_rows = (
    df_order_items
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.product_id") == F.col("k.product_id")), "inner")
    .select("s.*")
)

enriched = (
    kept_rows
    .alias("e")
    .join(df_orders.alias("o"), F.col("e.order_id") == F.col("o.id"), "left")
    .select("e.*", F.col("o.shipping_fee"))
)

enriched_order_items = (
    enriched
    .alias("a")
    .join(df_products.alias("d"), F.col("a.product_id") == F.col("d.id"), "left")
    .select("a.*", F.col("d.brand"), F.col("d.category"))
)

filtered = (
    enriched_order_items
    .filter(F.col("product_id").isNotNull())
    .select(
        "*",
        F.round((F.col("quantity") * 1.1), 2).alias("adjusted_quantity"),
        F.coalesce("brand", F.lit("UNKNOWN")).alias("brand_clean"),
    )
)

matched_keys_2 = (
    filtered
    .select("product_id", "order_id")
    .subtract(
        df_order_items
        .filter(F.col("unit_price").between(230, 340))
        .select("product_id", "order_id")
    )
)

kept_rows_order_items = (
    filtered
    .alias("s")
    .join(matched_keys_2.alias("k"), (F.col("s.product_id") == F.col("k.product_id")) & (F.col("s.order_id") == F.col("k.order_id")), "inner")
    .select("s.*")
)

joined = (
    kept_rows_order_items
    .alias("a")
    .join(df_orders.alias("b"), F.col("a.order_id") == F.col("b.id"), "left")
    .select("a.*", F.col("b.status"), F.col("b.amount"))
)

result = (
    joined
    .withColumn("adjusted_quantity_tier", F.when(F.col("adjusted_quantity") >= 62, "PREMIUM").otherwise("STANDARD"))
    .rollup("adjusted_quantity_tier", "brand")
    .agg(
        F.stddev_samp("quantity").alias("quantity_stddev"),
        F.sum(F.when(F.col("category") == "Fashion", F.col("unit_price")).otherwise(0)).alias("fashion_unit_price"),
        F.sum(F.when(F.col("status") == "SHIPPED", F.col("quantity"))).alias("shipped_total_quantity"),
        F.round(F.sum("shipping_fee") / F.nullif(F.count(F.when(F.col("brand") == "Stark", 1)), F.lit(0)), 2).alias("shipping_fee_per_stark"),
        F.sum("shipping_fee").alias("total_shipping_fee"),
        F.grouping_id().alias("gid"),
    )
    .orderBy("brand")
)
