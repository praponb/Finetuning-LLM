from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.col("start_date") <= "2024-02-01")
    .select(
        "plan",
        "end_date",
        "customer_id",
        F.floor("monthly_fee").alias("monthly_fee_whole"),
    )
)
