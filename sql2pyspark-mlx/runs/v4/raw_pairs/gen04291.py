from pyspark.sql import functions as F

result = (
    df_shipments
    .groupBy("status")
    .agg(F.max("cost").alias("max_cost"))
    .filter(F.col("max_cost") >= 31)
    .select("status", "max_cost")
    .orderBy("max_cost")
)
