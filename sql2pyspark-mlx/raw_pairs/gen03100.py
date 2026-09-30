from pyspark.sql import functions as F

remaining_keys = (
    df_order_items
    .select("product_id")
    .intersectAll(
        df_order_items
        .filter(F.col("quantity") > 5)
        .select("product_id")
    )
)

reconciled = (
    df_order_items
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.product_id") == F.col("k.product_id")), "inner")
    .select("s.*")
)

result = reconciled.select("order_id", "unit_price")
