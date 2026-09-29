from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.col("method").isNotNull() & (F.col("order_id").isNotNull() | F.col("amount").between(810, 1560)))
    .select("id", "paid_at")
    .orderBy(F.col("paid_at").desc(), "id")
    .limit(3)
)
