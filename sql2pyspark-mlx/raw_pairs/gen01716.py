from pyspark.sql import functions as F

result = (
    df_shipments.alias("s")
    .join(df_orders.alias("o"), F.col("s.order_id") == F.col("o.id"), "left")
    .groupBy(F.col("o.status"), F.col("o.channel"))
    .agg(F.sort_array(F.collect_set(F.col("s.carrier"))).alias("carrier_list"))
)
