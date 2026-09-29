from pyspark.sql import functions as F

result = (
    df_subscriptions
    .groupBy(F.date_trunc("MONTH", "start_date").alias("start_month"))
    .agg(
        F.sum(F.when(F.col("status") == "ACTIVE", 1).otherwise(0)).alias("active_count"),
    )
    .filter(F.col("active_count") >= 52)
    .select("start_month", "active_count")
    .orderBy("active_count")
)
