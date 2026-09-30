from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.col("end_date").between("2024-08-11", "2024-12-01"))
    .select(
        "plan",
        F.when(F.col("monthly_fee") >= 241, "High").when(F.col("monthly_fee") >= 86, "Medium").otherwise("Low").alias("monthly_fee_level"),
    )
)
