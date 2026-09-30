from pyspark.sql import functions as F

result = (
    df_shipments
    .groupBy("carrier", "status")
    .agg(F.avg("cost").alias("avg_cost"))
    .filter(F.col("avg_cost") >= 27)
    .select("carrier", "status", "avg_cost")
    .orderBy("status")
)
