from pyspark.sql import functions as F

result = (
    df_payments
    .groupBy(F.quarter("paid_at").alias("paid_quarter"), "method")
    .agg(
        F.max("paid_at").alias("last_paid_at"),
        F.count("amount").alias("amount_count"),
    )
    .filter(F.col("amount_count") > 2)
    .orderBy("method")
)
