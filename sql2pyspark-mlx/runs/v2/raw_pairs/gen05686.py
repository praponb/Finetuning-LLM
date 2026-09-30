from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.col("customer_id").isin([34, 36]))
    .select(
        "customer_id",
        "start_date",
        "status",
        F.when(F.col("monthly_fee") >= 249, "High").when(F.col("monthly_fee") >= 71, "Medium").otherwise("Low").alias("monthly_fee_level"),
    )
    .orderBy("start_date")
)
