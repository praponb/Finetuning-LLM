from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter((F.col("plan") != "standard") & (F.year("start_date") == 2024))
    .groupBy(
        F.when(F.col("plan").isin(["basic", "standard"]), "tier_1").otherwise("other").alias("plan_group"),
        "plan",
    )
    .agg(
        F.round(((100 * F.sum(F.when(F.col("plan") == "premium", 1).otherwise(0))) / F.count("*")), 1).alias("pct_premium"),
        F.countDistinct("monthly_fee").alias("unique_customers"),
    )
)
