from pyspark.sql import functions as F

base = (
    df_subscriptions
    .filter(F.col("customer_id").isNull() & F.col("start_date").between("2023-06-01", "2024-11-01"))
    .withColumn("start_date_period", F.date_trunc("MONTH", "start_date"))
)

summary = (
    base
    .groupBy(F.date_format("end_date", "yyyy-MM").alias("end_month"))
    .agg(
        F.sum(F.when(F.col("plan") == "standard", 1).otherwise(0)).alias("standard_count"),
        (F.max("monthly_fee") - F.min("monthly_fee")).alias("monthly_fee_range"),
    )
)

result = summary.orderBy(F.col("monthly_fee_range").desc())
