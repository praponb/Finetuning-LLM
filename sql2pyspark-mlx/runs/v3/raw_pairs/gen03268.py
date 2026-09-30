from pyspark.sql import functions as F

result = (
    df_sessions
    .filter((F.col("customer_id") == 5) | F.col("customer_id").isin([16, 32]))
    .select("id", "started_at", "country", "customer_id")
)
