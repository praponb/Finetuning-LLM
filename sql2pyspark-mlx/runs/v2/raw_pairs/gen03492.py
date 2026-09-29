from pyspark.sql import functions as F

result = (
    df_order_items.alias("order_items")
    .join(df_orders.filter(F.col("discount").between(0.24, 0.33)).alias("orders"), F.col("order_items.order_id") == F.col("orders.id"), "left_anti")
    .select(F.col("order_items.id"), F.col("order_items.unit_price"))
    .orderBy(F.col("id").desc(), "unit_price")
    .limit(3)
)
