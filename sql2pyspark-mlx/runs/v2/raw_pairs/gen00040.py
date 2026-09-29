from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.col("paid_at") < "2025-01-01")
    .select("method", "order_id")
    .intersect(
        df_payments
        .filter(~F.col("method").isin(["promptpay", "paypal"]))
        .select("method", "order_id")
    )
)
