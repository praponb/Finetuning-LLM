from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.col("order_id").isin([20, 33]) & F.hour("paid_at").between(6, 11))
    .select("id", "paid_at", F.upper("status").alias("status_upper"))
    .orderBy(F.col("id").desc())
    .limit(10)
)
