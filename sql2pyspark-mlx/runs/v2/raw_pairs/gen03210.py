from pyspark.sql import functions as F

result = (
    df_order_items.alias("order_items")
    .filter(F.col("order_items.product_id").isNotNull())
    .join(df_products.filter(F.col("stock_qty").between(232, 376)).alias("products"), F.col("order_items.product_id") == F.col("products.id"), "left_anti")
    .select(
        F.col("order_items.id"),
        F.col("order_items.unit_price"),
        F.col("order_items.product_id"),
        F.col("order_items.quantity"),
    )
)
