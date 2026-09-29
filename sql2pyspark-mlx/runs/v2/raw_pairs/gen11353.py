from pyspark.sql import functions as F

base = (
    df_subscriptions
    .filter((F.col("monthly_fee").isNotNull() & (F.year("start_date") == 2023)) | (F.col("customer_id") == 28))
)

summary = (
    base
    .groupBy(F.quarter("end_date").alias("end_quarter"))
    .agg(F.max("end_date").alias("latest_end_date"))
)

result = (
    summary
    .select("end_quarter", "latest_end_date")
    .orderBy(F.col("end_quarter").desc(), "latest_end_date")
)
