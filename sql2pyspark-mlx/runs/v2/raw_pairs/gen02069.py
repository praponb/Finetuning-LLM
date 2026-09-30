from pyspark.sql import functions as F

result = (
    df_payments
    .filter((F.col("paid_at") < "2023-03-20") & F.col("order_id").isin([2, 10]))
    .select("amount", F.upper("status").alias("status_upper"))
    .orderBy("amount")
)
