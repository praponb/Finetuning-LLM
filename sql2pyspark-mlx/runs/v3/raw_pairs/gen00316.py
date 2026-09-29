from pyspark.sql import functions as F

result = (
    df_order_items.alias("order_items")
    .join(df_products.alias("products"), F.col("order_items.product_id") == F.col("products.id"), "inner")
    .filter(F.upper(F.col("products.product_name")).like("%ESK%"))
    .select(
        F.col("order_items.id"),
        F.col("order_items.order_id"),
        F.col("products.price"),
        F.col("products.category"),
        F.col("products.stock_qty"),
    )
    .orderBy(F.col("stock_qty").desc(), F.col("price").desc())
)
