from pyspark.sql import functions as F

result = (
    df_sessions
    .filter((F.to_date("started_at") >= "2025-03-01") & (F.col("browser") == "Safari"))
    .orderBy("country", "customer_id")
)
