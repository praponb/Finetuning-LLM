from pyspark.sql import functions as F

result = (
    df_payments
    .filter(~(~F.col("status").isin(["FAILED", "REFUNDED"]) & F.col("order_id").isin([22, 24])))
    .select("paid_at", "method", "status")
    .orderBy("method")
)
