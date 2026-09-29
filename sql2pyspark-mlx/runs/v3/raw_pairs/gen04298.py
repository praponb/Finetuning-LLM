from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.col("order_id").isin([13, 30, 40]) & ((F.col("amount") < 800) | (F.col("paid_at") < "2023-05-09")))
    .groupBy("method")
    .agg(
        F.min("paid_at").alias("min_paid_at"),
        (F.max("amount") - F.min("amount")).alias("amount_range"),
    )
    .filter(F.col("amount_range") >= 41)
)
