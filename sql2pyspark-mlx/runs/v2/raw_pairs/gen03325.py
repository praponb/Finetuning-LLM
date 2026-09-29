from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_customers.alias("c"), F.col("c.id") == F.col("o.customer_id"), "inner")
    .join(df_stores.alias("s"), F.col("o.store_id") == F.col("s.id"), "inner")
    .groupBy(F.col("s.region"))
    .agg(F.sort_array(F.collect_set(F.col("o.status"))).alias("status_list"))
    .orderBy("region")
)
