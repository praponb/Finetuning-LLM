from pyspark.sql import functions as F

result = (
    df_payments
    .filter((F.col("method") != "cash") & (F.col("order_id") == 24) & (F.col("status") == "SUCCESS"))
    .select("id", "status", "method", "paid_at")
)
