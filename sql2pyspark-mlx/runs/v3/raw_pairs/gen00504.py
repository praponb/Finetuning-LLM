from pyspark.sql import functions as F

result = (
    df_payments
    .filter(~F.col("status").isin(["FAILED", "SUCCESS", "REFUNDED"]) & F.col("order_id").isNotNull() & (F.col("paid_at") < "2024-08-14"))
    .select("paid_at")
    .orderBy("paid_at")
)
