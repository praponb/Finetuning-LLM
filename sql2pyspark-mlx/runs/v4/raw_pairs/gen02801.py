from pyspark.sql import functions as F

result = (
    df_sessions
    .filter(F.col("country").isin(["JP", "UK"]) & (F.col("device") != "desktop") & (F.to_date("started_at") < "2024-10-01"))
    .select("id", "country", "customer_id", "started_at")
)
