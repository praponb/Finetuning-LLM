from pyspark.sql import functions as F

result = (
    df_subscriptions
    .groupBy(
        "plan",
        F.when(F.col("plan").isin(["basic", "enterprise"]), "group_a").otherwise("other").alias("plan_group"),
        F.when(F.col("monthly_fee") >= 214, "high").when(F.col("monthly_fee") >= 53, "mid").otherwise("low").alias("monthly_fee_bucket"),
    )
    .agg(
        F.sum("monthly_fee").alias("total_monthly_fee"),
        F.countDistinct("monthly_fee").alias("unique_customers"),
    )
)
