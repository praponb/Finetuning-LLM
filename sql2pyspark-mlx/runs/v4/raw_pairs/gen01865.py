from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.hour("paid_at").between(9, 12) & (F.col("method") == "bank_transfer"))
    .select("method", "status", "amount", F.length("method").alias("method_length"))
)
