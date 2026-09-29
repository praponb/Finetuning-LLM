from pyspark.sql import functions as F

result = (
    df_order_items.alias("order_items")
    .join(df_products.filter(F.col("product_name").like("%mous%")).alias("products"), F.col("order_items.product_id") == F.col("products.id"), "left_semi")
    .select(
        F.col("order_items.id"),
        F.col("order_items.unit_price"),
        F.col("order_items.quantity"),
    )
    .orderBy(F.col("unit_price").desc(), F.col("id").desc())
)
