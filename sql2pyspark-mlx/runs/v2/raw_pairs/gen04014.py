from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_customers.alias("c"), F.col("o.customer_id") == F.col("c.id"), "left")
    .groupBy(F.col("c.segment"))
    .agg(F.sort_array(F.collect_set(F.col("o.status"))).alias("status_list"))
    .orderBy(F.col("segment").desc())
    .limit(5)
)
