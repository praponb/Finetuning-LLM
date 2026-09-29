from pyspark.sql import functions as F

result = (
    df_sessions
    .filter(F.col("country") == "JP")
    .select("id", "customer_id", "started_at", "device")
    .orderBy(F.col("started_at").desc(), "id")
    .limit(3)
)
