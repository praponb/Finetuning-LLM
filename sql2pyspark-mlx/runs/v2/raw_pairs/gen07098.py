from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.col("order_id").isin([6, 11, 20, 35]) | (F.col("order_id") == 11))
    .groupBy("status")
    .agg(
        F.sum(F.when(F.col("status") == "REFUNDED", F.col("amount")).otherwise(0)).alias("refunded_amount"),
    )
)
