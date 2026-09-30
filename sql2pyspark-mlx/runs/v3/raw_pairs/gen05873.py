from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.col("customer_id") == 40)
    .groupBy(
        F.month("start_date").alias("start_month"),
        F.quarter("start_date").alias("start_quarter"),
    )
    .agg(
        F.min("end_date").alias("min_end_date"),
        F.max("monthly_fee").alias("max_monthly_fee"),
    )
    .filter(F.col("max_monthly_fee") >= 230)
)
