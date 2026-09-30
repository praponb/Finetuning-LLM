from pyspark.sql import functions as F

result = (
    df_payments
    .filter((F.col("amount") > 990) & (F.col("status").isin(["SUCCESS", "REFUNDED", "FAILED"]) | F.col("amount").between(590, 1460)))
    .groupBy("status", "method", "order_id")
    .agg(F.min("paid_at").alias("first_paid_at"))
    .orderBy("method", "status", "order_id")
    .limit(10)
)
