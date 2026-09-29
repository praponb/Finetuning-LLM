from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.col("method") == "cash")
    .groupBy("order_id", "status")
    .agg(
        F.sum(F.when(F.col("status") == "REFUNDED", 1).otherwise(0)).alias("refunded_count"),
    )
    .filter(F.col("refunded_count") < 49)
    .select("order_id", "status", "refunded_count")
    .orderBy("status")
)
