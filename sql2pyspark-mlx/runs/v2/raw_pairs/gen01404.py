from pyspark.sql import functions as F

result = (
    df_order_items.alias("order_items")
    .join(df_products.alias("products"), F.col("order_items.product_id") == F.col("products.id"), "inner")
    .join(df_orders.alias("orders"), F.col("order_items.order_id") == F.col("orders.id"), "left")
    .filter(~(F.month(F.col("orders.order_date")).isin([5, 7, 11]) | F.col("orders.store_id").isin([35, 37])))
    .select(
        F.col("order_items.quantity"),
        F.col("products.price"),
        F.col("orders.order_date"),
    )
)
