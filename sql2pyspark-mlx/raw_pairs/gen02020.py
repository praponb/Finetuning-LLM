from pyspark.sql import functions as F

kept_keys = (
    df_order_items
    .select("product_id")
    .intersectAll(
        df_order_items
        .filter(F.col("quantity") != 5)
        .select("product_id")
    )
)

reconciled = (
    df_order_items
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.product_id") == F.col("k.product_id")), "inner")
    .select("s.*")
)

result = reconciled.select("id", "quantity", "unit_price")
