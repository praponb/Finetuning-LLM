from pyspark.sql import functions as F

result = (
    df_payments
    .groupBy("status", F.quarter("paid_at").alias("paid_quarter"))
    .agg(
        F.max("paid_at").alias("last_paid_at"),
        F.count("amount").alias("amount_count"),
    )
    .filter(F.col("amount_count") > 6)
    .orderBy(F.col("last_paid_at").desc())
)
