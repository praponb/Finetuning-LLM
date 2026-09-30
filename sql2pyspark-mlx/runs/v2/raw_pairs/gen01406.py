from pyspark.sql import functions as F

result = (
    df_payments
    .select("id", "order_id", "amount", F.to_date("paid_at").alias("paid_at_date"))
    .orderBy("amount")
)
