from pyspark.sql import functions as F

result = (
    df_shipments
    .groupBy("carrier")
    .agg(F.avg("cost").alias("avg_cost"))
    .orderBy("carrier", F.col("avg_cost").desc())
)
