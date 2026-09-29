from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.col("end_date") > "2025-05-01")
    .select(
        "id",
        "monthly_fee",
        F.when(F.col("monthly_fee") >= 270, "High").when(F.col("monthly_fee") >= 87, "Medium").otherwise("Low").alias("monthly_fee_level"),
        F.when(F.col("plan").isin(["enterprise", "premium"]), "Priority").otherwise("Other").alias("plan_group"),
    )
)
