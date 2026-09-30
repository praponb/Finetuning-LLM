from pyspark.sql import functions as F

result = (
    df_order_items.alias("order_items")
    .join(df_products.filter(F.col("price") > 990).alias("products"), F.col("order_items.product_id") == F.col("products.id"), "left_semi")
    .filter(F.col("order_items.order_id").isin([6, 30, 34]))
    .select(
        F.col("order_items.id"),
        F.col("order_items.quantity"),
        F.col("order_items.product_id"),
    )
)
