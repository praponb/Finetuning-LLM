from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter((F.col("status") == "CANCELLED") & (F.col("plan") == "standard"))
    .groupBy("plan")
    .agg(F.count(F.when(F.col("plan") == "premium", 1)).alias("premium_count"))
)
