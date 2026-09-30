from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("carrier") == "Flash")
    .groupBy("status")
    .agg(F.sum("cost").alias("total_cost"))
    .filter(F.col("total_cost") < 480)
    .select("status", "total_cost")
    .orderBy(F.col("total_cost").desc())
)
