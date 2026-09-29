from pyspark.sql import functions as F

result = (
    df_sessions
    .filter(F.col("customer_id") == 5)
    .select("id", "device", "browser", "started_at")
    .orderBy("browser", F.col("started_at").desc())
)
