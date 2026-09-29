from pyspark.sql import functions as F

remaining_keys = (
    df_order_items
    .select("product_id")
    .subtract(
        df_order_items
        .filter(F.col("unit_price").between(170, 290))
        .select("product_id")
    )
)

verified = (
    df_order_items
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.product_id") == F.col("k.product_id")), "inner")
    .select("s.*")
)

result = (
    verified
    .rollup(
        F.when(F.col("quantity") >= 7, "LARGE").otherwise("SMALL").alias("quantity_tier"),
    )
    .agg(
        F.percentile_approx("quantity", [0.25, 0.5, 0.75]).alias("quantity_quartiles"),
        F.percentile_approx("unit_price", [0.25, 0.5, 0.75]).alias("unit_price_quartiles"),
        F.countDistinct("product_id").alias("distinct_product_ids"),
        F.percentile_approx("quantity", 0.95).alias("quantity_p95"),
        F.percentile_approx("quantity", 0.5).alias("quantity_p50"),
    )
    .orderBy("quantity_p95")
)
