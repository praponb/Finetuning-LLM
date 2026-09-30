from pyspark.sql import functions as F

result = (
    df_subscriptions
    .groupBy(F.year("end_date").alias("end_year"))
    .agg(
        (F.max("monthly_fee") - F.min("monthly_fee")).alias("monthly_fee_range"),
        F.round(((100 * F.sum(F.when(F.col("status") == "EXPIRED", 1).otherwise(0))) / F.count("*")), 1).alias("pct_expired"),
    )
)
