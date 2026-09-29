from pyspark.sql import functions as F

result = (
    df_subscriptions
    .groupBy(
        "plan",
        F.when(F.col("status").isin(["PAUSED", "EXPIRED"]), "priority").otherwise("other").alias("status_group"),
    )
    .agg(F.min("monthly_fee").alias("min_monthly_fee"))
    .filter(F.col("min_monthly_fee") > 80)
)
