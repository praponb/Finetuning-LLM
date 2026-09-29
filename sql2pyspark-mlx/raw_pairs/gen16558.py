from pyspark.sql import functions as F

src = (
    df_subscriptions
    .filter(F.col("status").isin(["CANCELLED", "PAUSED"]) & F.col("monthly_fee").between(90, 170))
)

summary = (
    src
    .groupBy(
        "status",
        F.when(F.year("start_date") < 2024, "before_2024").when(F.year("start_date") == 2024, "2024").otherwise("after_2024").alias("start_cohort"),
    )
    .agg(
        F.round(((100 * F.sum(F.when(F.col("plan") == "premium", 1).otherwise(0))) / F.count("*")), 1).alias("pct_premium"),
        F.round(((100 * F.sum(F.when(F.col("status") == "EXPIRED", 1).otherwise(0))) / F.count("*")), 1).alias("pct_expired"),
    )
)

result = (
    summary
    .filter(F.col("pct_premium") > 65)
    .select("status", "start_cohort", "pct_premium", "pct_expired")
)
