from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.col("monthly_fee").between(80, 170) & (F.col("monthly_fee") >= 150) & (F.col("customer_id") == 35))
    .groupBy(F.quarter("end_date").alias("end_quarter"))
    .agg(
        F.sum(F.when(F.col("status") == "CANCELLED", 1).otherwise(0)).alias("cancelled_count"),
        F.sum(F.when(F.col("plan") == "enterprise", 1).otherwise(0)).alias("enterprise_count"),
    )
    .orderBy(F.col("enterprise_count").desc())
)
