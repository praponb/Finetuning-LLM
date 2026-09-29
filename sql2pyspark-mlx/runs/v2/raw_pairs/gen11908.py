from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.col("customer_id").isNotNull() & (F.col("plan") != "enterprise") & (F.col("status") == "ACTIVE"))
    .select(
        "status",
        "plan",
        F.when(F.col("monthly_fee") >= 192, "High").when(F.col("monthly_fee") >= 111, "Medium").otherwise("Low").alias("monthly_fee_tier"),
    )
    .orderBy(F.col("monthly_fee_tier").desc(), F.col("status").desc())
)
