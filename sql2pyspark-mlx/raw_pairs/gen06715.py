from pyspark.sql import functions as F

result = (
    df_payments
    .filter(((F.col("amount") <= 1060) & F.col("method").isin(["promptpay", "bank_transfer", "card"])) | F.col("order_id").isin([1, 28, 29]))
    .orderBy(F.col("id").desc())
    .limit(5)
)
