from pyspark.sql import functions as F

max_amount = df_payments.filter(F.col("order_id").isNull()).agg(F.max("amount")).first()[0]

result = (
    df_payments
    .filter(F.col("amount") >= max_amount)
    .select("id", "amount", "paid_at", "status")
    .orderBy(F.col("id").desc())
)
