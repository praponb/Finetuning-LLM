from pyspark.sql import functions as F

result = (
    df_order_items.alias("order_items")
    .join(df_products.alias("products"), F.col("order_items.product_id") == F.col("products.id"), "inner")
    .filter(F.col("products.stock_qty").between(151, 174))
    .select(
        F.col("order_items.product_id"),
        F.col("products.cost"),
        F.col("products.stock_qty"),
    )
    .orderBy(F.col("product_id").desc())
)
