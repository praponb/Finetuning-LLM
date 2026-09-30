from pyspark.sql import functions as F

result = (
    df_sessions
    .select("id", "device", F.length("device").alias("device_length"))
    .orderBy("device_length")
)
