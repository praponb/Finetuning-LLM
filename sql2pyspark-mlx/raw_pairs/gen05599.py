from pyspark.sql import functions as F

result = (
    df_shipments
    .select("id", "status", F.col("cost").cast("int").alias("cost_int"))
    .orderBy("status")
)
