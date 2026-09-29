from pyspark.sql import functions as F

result = (
    df_subscriptions
    .groupBy(
        F.when(F.col("status").isin(["EXPIRED", "ACTIVE"]), "priority").otherwise("other").alias("status_group"),
    )
    .agg(
        F.sum(F.when(F.col("plan") == "enterprise", F.col("monthly_fee")).otherwise(0)).alias("enterprise_monthly_fee"),
        F.min("monthly_fee").alias("min_monthly_fee"),
    )
    .orderBy(F.col("min_monthly_fee").desc())
)
