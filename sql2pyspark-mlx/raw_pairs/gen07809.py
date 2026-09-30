from pyspark.sql import functions as F

result = (
    df_subscriptions
    .groupBy(F.year("end_date").alias("end_year"), "status")
    .agg(F.count("monthly_fee").alias("monthly_fee_count"))
    .filter(F.col("monthly_fee_count") >= 7)
)
