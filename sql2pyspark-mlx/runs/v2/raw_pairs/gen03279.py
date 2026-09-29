from pyspark.sql import functions as F

result = (
    df_shipments.alias("s")
    .join(df_orders.alias("o"), F.col("s.order_id") == F.col("o.id"), "outer")
    .join(df_customers.alias("c"), F.col("c.id") == F.col("o.customer_id"), "right")
    .groupBy(F.col("c.city"))
    .agg(F.sort_array(F.collect_set(F.col("s.carrier"))).alias("carrier_list"))
    .orderBy("city")
)
