from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.year("start_date") >= 2023)
    .groupBy("status", "plan", F.year("end_date").alias("end_year"))
    .agg(
        F.sum(F.when(F.col("plan") == "basic", 1).otherwise(0)).alias("basic_count"),
        F.round(((100 * F.sum(F.when(F.col("status") == "PAUSED", 1).otherwise(0))) / F.count("*")), 1).alias("pct_paused"),
    )
)
