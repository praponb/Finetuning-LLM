from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter((F.col("end_date") <= "2023-09-20") & (F.col("monthly_fee") >= 170))
    .groupBy("status")
    .agg((F.max("monthly_fee") - F.min("monthly_fee")).alias("monthly_fee_range"))
)
