from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.col("status").isin(["PAUSED", "ACTIVE", "CANCELLED"]) & ((F.col("plan") == "standard") | (F.col("end_date") <= "2024-11-01")))
    .select(
        "id",
        "monthly_fee",
        "end_date",
        F.coalesce("monthly_fee", F.lit(0)).alias("monthly_fee_filled"),
        F.ceil("monthly_fee").alias("monthly_fee_whole"),
    )
    .orderBy(F.col("id").desc())
    .limit(20)
)
