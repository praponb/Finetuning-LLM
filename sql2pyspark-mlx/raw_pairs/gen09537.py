from pyspark.sql import functions as F

remaining_keys = (
    df_order_items
    .select("product_id", "order_id")
    .subtract(
        df_order_items
        .filter(F.col("order_id").isin([16, 28]))
        .select("product_id", "order_id")
    )
)

reconciled = (
    df_order_items
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.product_id") == F.col("k.product_id")) & (F.col("s.order_id") == F.col("k.order_id")), "inner")
    .select("s.*")
)

result = (
    reconciled
    .select("order_id", "quantity", "unit_price")
    .orderBy(F.col("order_id").desc_nulls_last())
)
