from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.col("customer_id").isin([4, 35]) & F.month("start_date").isin([3, 7, 12]) & F.col("end_date").between("2023-04-01", "2023-04-18"))
    .select(
        "monthly_fee",
        "end_date",
        F.when(F.col("monthly_fee") >= 254, "High").when(F.col("monthly_fee") >= 107, "Medium").otherwise("Low").alias("monthly_fee_band"),
        F.when(F.col("plan") == "standard", 1).otherwise(0).alias("is_standard"),
    )
    .orderBy("monthly_fee_band", "end_date")
)
