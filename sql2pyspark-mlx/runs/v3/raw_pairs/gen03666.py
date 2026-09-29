from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.col("monthly_fee").between(160, 180))
    .groupBy("status")
    .agg(
        F.round(((100 * F.sum(F.when(F.col("status") == "CANCELLED", 1).otherwise(0))) / F.count("*")), 1).alias("pct_cancelled"),
    )
)
