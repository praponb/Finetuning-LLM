from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.col("monthly_fee").between(160, 220))
    .agg(F.count(F.when(F.col("plan") == "premium", 1)).alias("premium_count"))
    .orderBy(F.col("premium_count").desc())
)
