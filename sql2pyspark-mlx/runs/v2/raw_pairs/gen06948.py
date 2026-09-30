from pyspark.sql import functions as F

result = (
    df_sessions
    .filter(F.col("device").isin(["mobile", "tablet"]))
    .select("id", "country", F.length("device").alias("device_length"))
)
