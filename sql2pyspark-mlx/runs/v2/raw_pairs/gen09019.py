from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.col("customer_id").isNull() & F.col("end_date").between("2023-08-09", "2025-10-01"))
    .agg(F.countDistinct("monthly_fee").alias("unique_statuss"))
    .orderBy(F.col("unique_statuss").desc())
)
