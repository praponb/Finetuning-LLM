from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.hour("paid_at").between(11, 14) & (F.hour("paid_at").between(7, 11) | (F.col("amount") >= 1300)))
    .select("id", "method", "status", F.length("status").alias("status_length"))
)
