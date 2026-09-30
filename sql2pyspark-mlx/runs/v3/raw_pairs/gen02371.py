from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.col("end_date").between("2024-09-01", "2024-09-28") & F.col("customer_id").isNotNull())
    .groupBy("customer_id", "status")
    .agg(
        F.min("start_date").alias("min_start_date"),
        F.avg("monthly_fee").alias("avg_monthly_fee"),
        F.count("*").alias("cnt"),
    )
    .orderBy(F.col("customer_id").desc())
)
