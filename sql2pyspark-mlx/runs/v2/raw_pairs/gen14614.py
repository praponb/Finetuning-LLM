from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.col("method").isNull() & F.col("amount").between(700, 1380))
    .select("paid_at", "amount", "order_id")
    .orderBy(F.col("amount").desc())
)
