from pyspark.sql import functions as F

result = (
    df_order_items.alias("order_items")
    .join(df_products.alias("products"), F.col("order_items.product_id") == F.col("products.id"), "outer")
    .filter(F.col("products.cost") >= 610)
    .select(
        F.col("order_items.product_id"),
        F.col("products.product_name"),
        F.col("products.stock_qty"),
        F.col("products.category"),
    )
)
