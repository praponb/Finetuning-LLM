from pyspark.sql import functions as F

result = (
    df_sessions
    .filter(~(F.col("country").isin(["TH", "JP", "DE"]) & (F.col("started_at") >= "2024-02-01")))
    .select("started_at", "customer_id", "country")
)
