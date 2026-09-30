from pyspark.sql import functions as F

result = (
    df_payments
    .filter(((F.col("status") == "FAILED") & (F.col("method") != "promptpay")) | (F.to_date("paid_at") < "2023-12-07"))
    .select("id", "method", "status")
)
