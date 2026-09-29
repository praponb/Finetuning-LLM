from pyspark.sql import functions as F

result = (
    df_subscriptions
    .groupBy(
        F.when(F.col("status").isin(["CANCELLED", "EXPIRED"]), "priority").otherwise("other").alias("status_group"),
    )
    .agg(F.count("monthly_fee").alias("monthly_fee_count"))
    .filter(F.col("monthly_fee_count") < 1)
)
