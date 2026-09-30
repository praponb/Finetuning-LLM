from pyspark.sql import functions as F

result = (
    df_shipments
    .groupBy("carrier")
    .agg(F.max("cost").alias("max_cost"))
    .filter(F.col("max_cost") >= 75)
    .orderBy(F.col("max_cost").desc())
)
