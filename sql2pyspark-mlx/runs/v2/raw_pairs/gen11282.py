from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.col("start_date") >= "2024-01-11")
    .groupBy("plan")
    .agg(F.count(F.when(F.col("plan") == "premium", 1)).alias("premium_count"))
    .filter(F.col("premium_count") >= 6)
    .select("plan", "premium_count")
    .orderBy("plan")
)
