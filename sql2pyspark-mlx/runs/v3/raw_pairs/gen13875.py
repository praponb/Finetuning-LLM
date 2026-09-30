from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter((F.col("end_date") < "2025-04-01") & F.col("customer_id").isNull())
    .groupBy("plan")
    .agg(
        F.sum("monthly_fee").alias("total_monthly_fee"),
        (F.max("monthly_fee") - F.min("monthly_fee")).alias("monthly_fee_range"),
        F.max("monthly_fee").alias("max_monthly_fee"),
    )
    .filter(F.col("monthly_fee_range") >= 23)
)
