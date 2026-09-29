from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("order_id") == 33)
    .groupBy("status")
    .agg(F.avg("cost").alias("avg_cost"))
    .filter(F.col("avg_cost") >= 27)
    .orderBy(F.col("avg_cost").desc(), "status")
    .limit(10)
)
