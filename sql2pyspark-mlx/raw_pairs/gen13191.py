from pyspark.sql import functions as F

result = (
    df_sessions
    .select(F.col("country").alias("label"))
    .unionByName(df_shipments.select(F.col("status").alias("label")))
)
