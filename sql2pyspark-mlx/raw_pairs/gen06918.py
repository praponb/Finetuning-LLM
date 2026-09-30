from pyspark.sql import functions as F

result = (
    df_sessions
    .filter(F.col("country").isin(["US", "DE"]) & F.hour("started_at").between(10, 15))
    .select("id", "browser", "started_at", F.length("device").alias("device_length"))
)
