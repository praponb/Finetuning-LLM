from pyspark.sql import functions as F

base = (
    df_subscriptions
    .filter((F.col("monthly_fee") > 190) | (F.col("customer_id") == 11))
    .withColumn("months_end_date_to_start_date", F.round((F.datediff("start_date", "end_date") / 30), 1))
)

summary = (
    base
    .groupBy(F.date_trunc("MONTH", "start_date").alias("start_month"))
    .agg(
        F.countDistinct("months_end_date_to_start_date").alias("unique_statuss"),
        F.count("months_end_date_to_start_date").alias("months_end_date_to_start_date_count"),
    )
)

result = (
    summary
    .filter(F.col("unique_statuss") >= 3)
    .select("start_month", "unique_statuss", "months_end_date_to_start_date_count")
)
