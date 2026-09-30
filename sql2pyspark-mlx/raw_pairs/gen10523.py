from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.col("start_date").between("2023-06-01", "2025-01-01"))
    .withColumn("start_date_period", F.date_trunc("MONTH", "start_date"))
    .orderBy(F.col("id").desc())
    .limit(3)
)
