from pyspark.sql import functions as F

result = (
    df_sessions
    .select("started_at", "country", "customer_id")
    .orderBy(F.col("started_at").desc())
)
