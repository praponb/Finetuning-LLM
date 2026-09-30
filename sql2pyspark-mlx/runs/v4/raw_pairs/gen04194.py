from pyspark.sql import functions as F

result = (
    df_sessions
    .filter(F.col("customer_id") == 23)
    .select("id", "country", F.month("started_at").alias("started_at_month"))
)
