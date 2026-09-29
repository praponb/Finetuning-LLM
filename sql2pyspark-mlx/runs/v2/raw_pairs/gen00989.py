from pyspark.sql import functions as F

result = (
    df_subscriptions
    .groupBy("plan")
    .agg(
        F.min("start_date").alias("min_start_date"),
        F.countDistinct("monthly_fee").alias("unique_customers"),
    )
    .filter(F.col("unique_customers") >= 6)
)
