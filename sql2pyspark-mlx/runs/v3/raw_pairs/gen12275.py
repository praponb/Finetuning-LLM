from pyspark.sql import functions as F

result = (
    df_shipments
    .groupBy("carrier", "order_id")
    .agg(F.count(F.when(F.col("status") == "RETURNED", 1)).alias("returned_count"))
    .orderBy("carrier", F.col("returned_count").desc())
)
