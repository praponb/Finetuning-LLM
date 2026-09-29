from pyspark.sql import functions as F

result = (
    df_shipments
    .groupBy("status")
    .agg(F.max("cost").alias("max_cost"))
    .filter(F.col("max_cost") >= 64)
    .orderBy("status", F.col("max_cost").desc())
)
