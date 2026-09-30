from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.col("order_id").isin([1, 10, 33, 39]) & ((F.col("amount") <= 900) | (F.col("method") != "bank_transfer")))
    .groupBy(F.hour("paid_at").alias("paid_hour"), "method")
    .agg(
        F.max("amount").alias("max_amount"),
        F.min("paid_at").alias("first_paid_at"),
    )
    .orderBy(F.col("paid_hour").desc(), "first_paid_at", "method")
    .limit(5)
)
