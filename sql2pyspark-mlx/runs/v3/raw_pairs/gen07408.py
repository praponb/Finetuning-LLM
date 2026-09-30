from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.col("plan").isin(["basic", "standard"]) & (F.col("start_date") <= "2024-12-01"))
    .groupBy(
        F.when(F.col("plan").isin(["basic", "enterprise"]), "core").otherwise("other").alias("plan_group"),
    )
    .agg(
        F.min("start_date").alias("min_start_date"),
        F.avg("monthly_fee").alias("avg_monthly_fee"),
    )
    .orderBy(F.col("min_start_date").desc())
)
