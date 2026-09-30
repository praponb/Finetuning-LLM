from pyspark.sql import functions as F

recent_subscriptions = (
    df_subscriptions
    .filter(F.col("status") == "CANCELLED")
    .withColumn("end_date_weekofyear", F.weekofyear("end_date"))
)

agg = (
    recent_subscriptions
    .groupBy("status")
    .agg(F.min("end_date_weekofyear").alias("min_end_date_weekofyear"))
)

result = (
    agg
    .select("status", "min_end_date_weekofyear")
    .orderBy(F.col("min_end_date_weekofyear").desc())
)
