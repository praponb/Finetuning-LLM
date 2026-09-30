from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.col("customer_id").isin([7, 12, 25]))
    .select(
        "id",
        "plan",
        "monthly_fee",
        "start_date",
        F.initcap(F.trim("plan")).alias("plan_title"),
        F.when(F.col("monthly_fee") >= 258, "High").when(F.col("monthly_fee") >= 96, "Medium").otherwise("Low").alias("monthly_fee_level"),
    )
    .orderBy(F.col("monthly_fee").desc())
)
