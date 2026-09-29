from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.month("start_date").isin([5, 6, 8]))
    .groupBy(F.date_trunc("MONTH", "start_date").alias("start_month"), "status")
    .agg(
        F.max("start_date").alias("last_start_date"),
        (F.max("monthly_fee") - F.min("monthly_fee")).alias("monthly_fee_range"),
    )
    .filter(F.col("monthly_fee_range") > 62)
    .orderBy("monthly_fee_range")
)
