from pyspark.sql import functions as F

remaining_keys = (
    df_order_items
    .select("id", "product_id")
    .subtract(
        df_order_items
        .filter(F.col("quantity") >= 8)
        .select("id", "product_id")
    )
)

reconciled = (
    df_order_items
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.product_id") == F.col("k.product_id")), "inner")
    .select("s.*")
)

result = (
    reconciled
    .groupBy("order_id")
    .agg(
        F.avg("unit_price").alias("avg_unit_price"),
        F.bit_and("quantity").alias("quantity_bit_and"),
        F.round(F.avg("quantity"), 2).alias("avg_quantity"),
        F.corr("quantity", "unit_price").alias("corr_quantity_unit_price"),
        F.bit_or("quantity").alias("quantity_bit_or"),
    )
)
