from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.col("method").isin(["promptpay", "bank_transfer", "card"]))
    .select("method", "status")
    .distinct()
    .orderBy("method", "status")
    .limit(20)
)
