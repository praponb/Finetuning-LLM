from pyspark.sql import functions as F

result = (
    df_order_items.alias("order_items")
    .join(df_products.alias("products"), F.col("order_items.product_id") == F.col("products.id"), "inner")
    .select(
        F.col("order_items.id"),
        F.col("order_items.unit_price"),
        F.col("order_items.product_id"),
        F.col("products.stock_qty"),
    )
)
