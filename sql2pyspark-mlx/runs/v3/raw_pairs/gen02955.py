from pyspark.sql import functions as F

result = (
    df_subscriptions
    .groupBy(F.when(F.col("plan").isin(["standard", "enterprise"]), "core").otherwise("other").alias("plan_group"))
    .pivot("status", ["CANCELLED", "ACTIVE", "EXPIRED"])
    .agg(F.sum("monthly_fee"))
)
