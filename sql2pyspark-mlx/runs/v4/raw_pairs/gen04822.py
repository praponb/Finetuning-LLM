from pyspark.sql import functions as F

result = (
    df_subscriptions
    .select(
        "id",
        "plan",
        "start_date",
        F.when(F.col("monthly_fee") >= 223, "High").when(F.col("monthly_fee") >= 81, "Medium").otherwise("Low").alias("monthly_fee_band"),
    )
    .orderBy(F.col("id").desc())
    .limit(5)
)
