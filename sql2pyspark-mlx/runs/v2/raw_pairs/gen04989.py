from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter((F.col("status") == "EXPIRED") & (F.col("status") == "CANCELLED") & F.col("monthly_fee").isNotNull())
    .groupBy("customer_id", "status")
    .agg(F.min("start_date").alias("min_start_date"))
)
