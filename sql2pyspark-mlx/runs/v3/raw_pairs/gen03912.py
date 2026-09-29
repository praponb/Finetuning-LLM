from pyspark.sql import functions as F

result = (
    df_sessions
    .filter(F.col("device").isin(["desktop", "mobile"]) & F.col("country").isin(["DE", "SG"]) & (F.col("started_at") >= "2023-04-18"))
    .select("id", "country", "customer_id", F.lower("browser").alias("browser_lower"))
)
