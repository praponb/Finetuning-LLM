from pyspark.sql import functions as F

result = (
    df_subscriptions
    .select(
        "id",
        "plan",
        "status",
        "monthly_fee",
        F.when(F.col("monthly_fee") >= 268, "High").when(F.col("monthly_fee") >= 66, "Medium").otherwise("Low").alias("monthly_fee_band"),
    )
    .orderBy(F.col("monthly_fee_band").desc())
)
