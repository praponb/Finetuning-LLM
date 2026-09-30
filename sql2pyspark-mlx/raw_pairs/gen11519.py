from pyspark.sql import functions as F

result = (
    df_sessions
    .filter(F.col("customer_id").isNotNull() | (F.col("browser") == "Chrome"))
    .select("country", "customer_id", F.month("started_at").alias("started_at_month"))
    .orderBy(F.col("started_at_month").desc())
)
