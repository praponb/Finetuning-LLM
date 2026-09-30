from pyspark.sql import functions as F

result = (
    df_payments
    .filter((F.col("paid_at") < "2024-10-01") & (F.col("method") == "bank_transfer") & (F.col("method") == "bank_transfer"))
    .select("id", "method")
    .orderBy("method", F.col("id").desc())
)
