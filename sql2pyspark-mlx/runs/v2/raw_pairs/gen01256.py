from pyspark.sql import functions as F

result = (
    df_order_items.alias("order_items")
    .join(df_orders.alias("orders"), F.col("orders.id") == F.col("order_items.order_id"), "left")
    .filter((F.col("orders.shipping_fee") <= 20) & F.col("order_items.product_id").isin([8, 15, 26, 29]))
    .select(
        F.col("order_items.unit_price"),
        F.col("orders.order_date"),
        F.col("orders.status"),
        F.add_months(F.col("orders.order_date"), 12).alias("order_date_plus_months"),
    )
)
