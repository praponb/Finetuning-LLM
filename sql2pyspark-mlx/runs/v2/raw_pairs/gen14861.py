from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.month("end_date").isin([3, 6, 11]))
    .groupBy("plan")
    .agg((F.max("monthly_fee") - F.min("monthly_fee")).alias("monthly_fee_range"))
    .filter(F.col("monthly_fee_range") >= 64)
    .orderBy("plan", F.col("monthly_fee_range").desc())
)
