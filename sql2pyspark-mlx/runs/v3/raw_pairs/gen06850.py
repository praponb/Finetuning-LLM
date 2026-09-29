from pyspark.sql import functions as F

result = (
    df_orders.alias("orders")
    .join(df_order_items.filter(F.col("order_id").isin([9, 30])).alias("order_items"), F.col("orders.id") == F.col("order_items.order_id"), "left_anti")
    .select(F.col("orders.id"), F.col("orders.status"), F.col("orders.channel"))
    .orderBy("id")
    .limit(50)
)
