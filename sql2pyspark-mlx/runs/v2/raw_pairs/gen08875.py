from pyspark.sql import functions as F

result = (
    df_subscriptions
    .groupBy("status")
    .agg(F.count(F.when(F.col("plan") == "enterprise", 1)).alias("enterprise_count"))
    .filter(F.col("enterprise_count") < 2)
    .orderBy(F.col("enterprise_count").desc(), "status")
)
