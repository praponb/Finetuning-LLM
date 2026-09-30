from pyspark.sql import functions as F

result = (
    df_subscriptions
    .select(
        "id",
        "customer_id",
        F.when(F.col("monthly_fee") >= 205, "High").when(F.col("monthly_fee") >= 41, "Medium").otherwise("Low").alias("monthly_fee_band"),
    )
)
