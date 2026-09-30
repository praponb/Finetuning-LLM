from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("order_id").isin([11, 16]))
    .groupBy("carrier")
    .agg(F.count(F.when(F.col("status") == "LOST", 1)).alias("lost_count"))
    .orderBy("lost_count")
)
