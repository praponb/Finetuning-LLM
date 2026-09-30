from pyspark.sql import functions as F

result = (
    df_order_items.alias("order_items")
    .join(df_orders.alias("orders"), F.col("orders.id") == F.col("order_items.order_id"), "right")
    .join(df_customers.alias("customers"), F.col("customers.id") == F.col("orders.customer_id"), "inner")
    .filter(F.col("order_items.quantity").isNotNull())
    .groupBy(
        F.month(F.col("orders.order_date")).alias("order_month"),
        F.col("orders.channel"),
    )
    .agg(
        F.sum((F.col("order_items.unit_price") * F.col("order_items.quantity"))).alias("revenue"),
    )
    .orderBy(F.col("order_month").desc(), "channel")
    .limit(10)
)
