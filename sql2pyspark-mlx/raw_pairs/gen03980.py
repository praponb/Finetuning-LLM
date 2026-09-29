from pyspark.sql import functions as F

result = (
    df_subscriptions
    .groupBy("plan", F.date_trunc("MONTH", "end_date").alias("end_month"))
    .agg(F.sum(F.when(F.col("plan") == "basic", 1).otherwise(0)).alias("basic_count"))
    .filter(F.col("basic_count") < 74)
    .select("plan", "end_month", "basic_count")
)
