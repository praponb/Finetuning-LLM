from pyspark.sql import functions as F

result = (
    df_subscriptions
    .groupBy("plan")
    .agg(F.count("*").alias("total_count"))
    .orderBy(F.col("total_count").desc(), "plan")
    .limit(5)
)
