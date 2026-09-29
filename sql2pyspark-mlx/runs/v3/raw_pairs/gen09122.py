from pyspark.sql import functions as F

result = (
    df_sessions
    .filter(F.col("customer_id").isNotNull())
    .select("id", "started_at", "customer_id", "browser")
    .orderBy("started_at")
)
