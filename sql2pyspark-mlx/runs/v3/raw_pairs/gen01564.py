from pyspark.sql import functions as F

result = (
    df_subscriptions
    .groupBy("plan")
    .agg(
        F.round(((100 * F.sum(F.when(F.col("status") == "EXPIRED", 1).otherwise(0))) / F.count("*")), 1).alias("pct_expired"),
        F.countDistinct("monthly_fee").alias("unique_statuss"),
    )
    .filter(F.col("pct_expired") >= 70)
    .select("plan", "pct_expired", "unique_statuss")
    .orderBy(F.col("unique_statuss").desc())
)
