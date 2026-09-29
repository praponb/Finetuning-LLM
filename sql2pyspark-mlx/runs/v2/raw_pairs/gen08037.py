from pyspark.sql import functions as F

result = (
    df_orders.alias("orders")
    .join(df_stores.alias("stores"), F.col("stores.id") == F.col("orders.store_id"), "outer")
    .join(df_customers.alias("customers"), F.col("customers.id") == F.col("orders.customer_id"), "right")
    .filter(~F.col("orders.channel").isin(["mobile", "store"]))
    .groupBy(F.col("stores.city"))
    .agg(F.max(F.col("orders.order_date")).alias("latest_order_date"))
    .orderBy("latest_order_date", "city")
    .limit(10)
)
