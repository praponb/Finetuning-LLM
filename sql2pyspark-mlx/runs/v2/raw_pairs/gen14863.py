from pyspark.sql import functions as F

result = (
    df_shipments.alias("s")
    .join(df_orders.alias("o"), F.col("s.order_id") == F.col("o.id"), "left")
    .filter(F.col("s.carrier") != "FedEx")
    .groupBy(F.col("o.status"))
    .agg(F.count("*").alias("total_count"))
    .orderBy(F.col("total_count").desc(), F.col("status").desc())
)
