from pyspark.sql import functions as F

result = (
    df_subscriptions
    .groupBy("status")
    .agg(
        F.min("monthly_fee").alias("min_monthly_fee"),
        F.max("end_date").alias("last_end_date"),
        F.count("*").alias("row_count"),
    )
    .filter(F.col("min_monthly_fee") > 230)
    .orderBy("last_end_date")
)
