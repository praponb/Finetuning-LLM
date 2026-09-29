from pyspark.sql import functions as F

result = (
    df_sessions
    .filter(F.hour("started_at").between(7, 15) & (F.col("customer_id") == 30) & (F.to_date("started_at") >= "2025-01-01"))
    .select("id", "customer_id", "started_at", "browser")
)
