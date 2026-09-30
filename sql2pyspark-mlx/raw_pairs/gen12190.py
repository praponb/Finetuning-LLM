from pyspark.sql import functions as F

base = (
    df_orders
    .filter(F.col("discount").isNull() | F.col("status").isin(["SHIPPED", "RETURNED", "PENDING"]))
)

orders_stats = (
    base
    .groupBy("channel", "customer_id")
    .agg(F.sort_array(F.collect_set("status")).alias("status_list"))
)

result = orders_stats.orderBy(F.col("customer_id").desc(), "channel")
