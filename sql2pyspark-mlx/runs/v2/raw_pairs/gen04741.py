from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.hour("paid_at").between(10, 13))
    .select("id", "amount", "method", "status")
    .orderBy(F.col("amount").desc())
)
