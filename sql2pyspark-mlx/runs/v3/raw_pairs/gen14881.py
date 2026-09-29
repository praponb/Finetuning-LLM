from pyspark.sql import functions as F

base = (
    df_subscriptions
    .filter(F.col("status").isin(["ACTIVE", "PAUSED"]) & (F.col("monthly_fee") > 230))
)

summary = (
    base
    .groupBy("status")
    .agg(F.count("*").alias("cnt"))
)

result = (
    summary
    .select("status", "cnt")
    .orderBy(F.col("cnt").desc())
)
