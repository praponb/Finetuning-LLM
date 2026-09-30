from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter((F.col("start_date") <= "2024-03-20") & F.col("start_date").between("2024-04-01", "2024-08-01"))
    .groupBy("plan", "status", "customer_id")
    .agg(
        F.sum("monthly_fee").alias("total_monthly_fee"),
        F.count("monthly_fee").alias("monthly_fee_count"),
    )
    .filter(F.col("monthly_fee_count") > 5)
    .drop("monthly_fee_count")
)
