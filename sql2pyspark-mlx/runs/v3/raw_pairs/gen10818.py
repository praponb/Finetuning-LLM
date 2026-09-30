from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(~(F.col("plan").isin(["basic", "enterprise"]) | (F.col("plan") == "premium")))
    .groupBy("status")
    .agg(
        F.countDistinct("monthly_fee").alias("unique_customers"),
        F.countDistinct("monthly_fee").alias("unique_statuss"),
    )
)
