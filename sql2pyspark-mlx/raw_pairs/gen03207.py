from pyspark.sql import functions as F

result = (
    df_sessions
    .filter(~(F.col("device").isin(["tablet", "mobile"]) & ~F.col("device").isin(["tablet", "desktop"]) & F.col("customer_id").isin([2, 14])))
    .select("started_at", "customer_id", F.trim("country").alias("clean_country"))
    .orderBy("started_at")
)
