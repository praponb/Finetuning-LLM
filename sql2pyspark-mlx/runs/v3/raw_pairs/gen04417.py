from pyspark.sql import functions as F

result = (
    df_shipments
    .groupBy("carrier")
    .agg(F.avg("cost").alias("avg_cost"))
    .orderBy("avg_cost", F.col("carrier").desc())
    .limit(10)
)
