from pyspark.sql import functions as F

result = (
    df_payments
    .groupBy("order_id", "method")
    .agg(F.count(F.when(F.col("status") == "REFUNDED", 1)).alias("refunded_count"))
    .orderBy(F.col("order_id").desc())
)
