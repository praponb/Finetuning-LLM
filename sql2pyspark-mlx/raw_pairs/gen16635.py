from pyspark.sql import functions as F

matched_keys = (
    df_order_items
    .select("product_id", "id")
    .exceptAll(
        df_order_items
        .filter(F.col("order_id") == 8)
        .select("product_id", "id")
    )
)

kept_rows = (
    df_order_items
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.product_id") == F.col("k.product_id")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

result = (
    kept_rows
    .select("id", "order_id", "product_id", "unit_price")
    .orderBy(F.col("unit_price").desc_nulls_first())
)
