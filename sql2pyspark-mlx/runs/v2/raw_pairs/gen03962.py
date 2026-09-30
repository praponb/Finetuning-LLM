from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter((F.col("monthly_fee") >= 180) & (F.col("customer_id").isin([3, 9, 31, 35]) | (F.col("plan") == "premium")))
    .select(
        "id",
        "end_date",
        "monthly_fee",
        "plan",
        F.when(F.col("status") == "PAUSED", 1).otherwise(0).alias("is_paused"),
    )
    .orderBy(F.col("monthly_fee").desc())
)
