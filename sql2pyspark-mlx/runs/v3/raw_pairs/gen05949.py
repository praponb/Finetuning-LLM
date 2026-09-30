from pyspark.sql import functions as F

result = (
    df_sessions
    .filter(F.col("started_at") >= "2023-06-12")
    .select("id", "customer_id", "device")
)
