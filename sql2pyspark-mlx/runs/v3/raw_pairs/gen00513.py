from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.col("order_id").isNotNull() & F.col("status").isin(["SUCCESS", "REFUNDED", "FAILED"]))
    .select("amount", "order_id")
    .orderBy(F.col("amount").desc())
)
