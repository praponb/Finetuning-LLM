from pyspark.sql import functions as F

result = (
    df_sessions
    .filter((F.col("device") == "mobile") & (F.col("customer_id") == 1))
    .orderBy("started_at", "id")
    .limit(1)
)
