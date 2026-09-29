from pyspark.sql import functions as F

result = (
    df_subscriptions
    .groupBy("plan")
    .agg(
        F.count(F.when(F.col("plan") == "enterprise", 1)).alias("enterprise_count"),
        F.count("*").alias("total_count"),
    )
    .orderBy(F.col("enterprise_count").desc(), F.col("total_count").desc(), "plan")
    .limit(5)
)
