from pyspark.sql import functions as F

src = (
    df_subscriptions
    .filter(F.col("start_date") < "2024-10-01")
    .withColumn("is_enterprise", F.when(F.col("plan") == "enterprise", 1).otherwise(0))
)

summary = (
    src
    .groupBy("plan")
    .agg(F.max("end_date").alias("latest_end_date"))
)

result = (
    summary
    .orderBy("plan")
    .limit(5)
)
