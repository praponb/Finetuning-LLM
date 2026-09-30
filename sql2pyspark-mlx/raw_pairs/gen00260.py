from pyspark.sql import functions as F

matched_keys = (
    df_order_items
    .select("product_id", "id")
    .intersectAll(
        df_order_items
        .filter(F.col("quantity") != 7)
        .select("product_id", "id")
    )
)

reconciled = (
    df_order_items
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.product_id") == F.col("k.product_id")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

result = (
    reconciled
    .select("id", "order_id", "product_id", "quantity")
    .orderBy(F.col("order_id").asc_nulls_last())
)
