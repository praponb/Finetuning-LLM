from pyspark.sql import functions as F

result = (
    df_payments
    .groupBy("status")
    .agg(
        F.max("amount").alias("max_amount"),
        F.count("*").alias("cnt"),
        F.min("paid_at").alias("min_paid_at"),
    )
    .filter(F.col("cnt") >= 4)
    .orderBy("status")
    .limit(5)
)
