from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.col("customer_id").isNull())
    .select(
        "status",
        "plan",
        "customer_id",
        F.when(F.col("monthly_fee") >= 208, "High").when(F.col("monthly_fee") >= 37, "Medium").otherwise("Low").alias("monthly_fee_band"),
    )
)
