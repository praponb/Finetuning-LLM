from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.col("monthly_fee").between(80, 170))
    .groupBy("plan")
    .agg(
        (F.max("monthly_fee") - F.min("monthly_fee")).alias("monthly_fee_range"),
        F.sum(F.when(F.col("status") == "CANCELLED", F.col("monthly_fee")).otherwise(0)).alias("cancelled_monthly_fee"),
    )
    .filter(F.col("cancelled_monthly_fee") > 57)
    .drop("cancelled_monthly_fee")
    .orderBy(F.col("monthly_fee_range").desc())
)
