from pyspark.sql import functions as F

result = (
    df_order_items.alias("order_items")
    .join(df_products.alias("products"), F.col("order_items.product_id") == F.col("products.id"), "inner")
    .filter((F.col("products.stock_qty") == 245) & F.col("order_items.unit_price").between(140, 210))
    .select(
        F.col("order_items.product_id"),
        F.col("order_items.quantity"),
        F.col("products.price"),
    )
)
