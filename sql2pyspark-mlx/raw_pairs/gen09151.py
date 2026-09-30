from pyspark.sql import functions as F

result = (
    df_payments
    .filter((F.col("method") == "card") & (F.col("paid_at") < "2025-06-08"))
    .select("id", "amount")
)
