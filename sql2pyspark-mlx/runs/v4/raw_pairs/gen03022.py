from pyspark.sql import functions as F

avg_amount = df_payments.agg(F.avg("amount")).first()[0]

result = (
    df_payments
    .filter(F.col("amount") >= avg_amount)
    .select("id", "amount", "order_id", "status")
    .orderBy(F.col("order_id").desc(), F.col("amount").desc(), "id")
    .limit(10)
)
