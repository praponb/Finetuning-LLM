from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("status") == "LOST")
    .select(F.col("status").alias("label"))
    .unionByName(df_sessions.select(F.col("country").alias("label")))
    .orderBy("label")
)
