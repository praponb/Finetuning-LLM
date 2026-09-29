from pyspark.sql import functions as F

result = (
    df_order_items.alias("order_items")
    .join(df_products.alias("products"), F.col("products.id") == F.col("order_items.product_id"), "right")
    .filter(F.col("products.price").between(320, 440))
    .select(
        F.col("order_items.id"),
        F.col("order_items.order_id"),
        F.col("order_items.unit_price"),
        F.col("products.brand"),
    )
    .orderBy(F.col("order_id").desc())
)
