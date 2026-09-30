from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.col("amount").between(620, 760) | (F.col("paid_at") < "2023-07-01"))
    .groupBy("order_id")
    .agg(
        F.sum(F.when(F.col("status") == "REFUNDED", 1).otherwise(0)).alias("refunded_count"),
    )
)
