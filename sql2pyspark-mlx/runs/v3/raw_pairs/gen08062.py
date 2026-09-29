from pyspark.sql import functions as F

result = (
    df_products.alias("products")
    .join(df_order_items.filter(F.col("order_id").isin([1, 7, 14, 19])).alias("order_items"), F.col("products.id") == F.col("order_items.product_id"), "left_semi")
    .select(
        F.col("products.id"),
        F.col("products.brand"),
        F.col("products.stock_qty"),
        F.col("products.category"),
    )
)
