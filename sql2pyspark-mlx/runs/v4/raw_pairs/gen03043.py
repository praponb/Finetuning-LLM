from pyspark.sql import functions as F

remaining_keys = (
    df_order_items
    .select("product_id", "id")
    .exceptAll(
        df_order_items
        .filter(F.col("quantity") < 5)
        .select("product_id", "id")
    )
)

kept_rows = (
    df_order_items
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.product_id") == F.col("k.product_id")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

result = (
    kept_rows
    .select("product_id", "quantity")
    .orderBy("quantity")
)
