from pyspark.sql import functions as F

recent_subscriptions = (
    df_subscriptions
    .filter(F.col("plan").isin(["premium", "basic"]) & (F.col("plan") == "basic"))
    .withColumn("clean_plan", F.trim("plan"))
)

agg = (
    recent_subscriptions
    .groupBy(
        F.date_trunc("MONTH", "start_date").alias("start_month"),
        F.when(F.col("monthly_fee") >= 247, "high").when(F.col("monthly_fee") >= 52, "mid").otherwise("low").alias("monthly_fee_bucket"),
    )
    .agg(
        F.sum(F.when(F.col("plan") == "standard", 1).otherwise(0)).alias("standard_count"),
        F.countDistinct("monthly_fee").alias("unique_customers"),
    )
)

result = (
    agg
    .filter(F.col("standard_count") > 64)
    .select("start_month", "monthly_fee_bucket", "standard_count", "unique_customers")
    .orderBy("monthly_fee_bucket", "start_month")
)
