from pyspark.sql import functions as F

remaining_keys = (
    df_order_items
    .select("product_id")
    .intersectAll(
        df_order_items
        .filter(F.col("quantity") >= 4)
        .select("product_id")
    )
)

reconciled = (
    df_order_items
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.product_id") == F.col("k.product_id")), "inner")
    .select("s.*")
)

result = (
    reconciled
    .groupBy(
        F.when(F.col("quantity") >= 8, "HIGH").otherwise("LOW").alias("quantity_tier"),
        F.when(F.col("unit_price") >= 430, "HIGH").otherwise("LOW").alias("unit_price_tier"),
    )
    .agg(
        F.round(F.avg("unit_price"), 2).alias("avg_unit_price"),
        F.corr("quantity", "unit_price").alias("corr_quantity_unit_price"),
    )
    .orderBy(F.col("avg_unit_price").desc_nulls_last())
)
