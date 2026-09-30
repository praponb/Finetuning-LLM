from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("status") != "DELIVERED")
    .groupBy("status")
    .agg(F.count("cost").alias("cost_count"))
    .filter(F.col("cost_count") < 9)
    .orderBy(F.col("cost_count").desc(), "status")
    .limit(10)
)
