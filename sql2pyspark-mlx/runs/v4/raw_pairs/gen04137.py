from pyspark.sql import functions as F

result = (
    df_payments
    .filter(((F.col("amount") > 610) & F.col("status").isin(["SUCCESS", "FAILED"])) | (F.to_date("paid_at") >= "2023-12-07"))
    .select("id", "amount", "status", "method")
)
