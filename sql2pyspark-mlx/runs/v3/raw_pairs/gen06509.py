from pyspark.sql import functions as F

result = (
    df_shipments.alias("s")
    .join(df_orders.alias("o"), F.col("s.order_id") == F.col("o.id"), "left")
    .filter(F.col("o.customer_id").isin([9, 12]))
    .groupBy(F.year(F.col("o.order_date")).alias("order_year"))
    .agg(F.sort_array(F.collect_set(F.col("s.status"))).alias("status_list"))
    .orderBy(F.col("order_year").desc())
)
