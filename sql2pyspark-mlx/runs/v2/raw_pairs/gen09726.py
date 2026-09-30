from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter((F.col("status") != "ACTIVE") & F.col("status").isin(["ACTIVE", "PAUSED", "CANCELLED"]))
    .select(
        "id",
        "end_date",
        F.when(F.col("monthly_fee") >= 263, "High").when(F.col("monthly_fee") >= 100, "Medium").otherwise("Low").alias("monthly_fee_tier"),
    )
)
