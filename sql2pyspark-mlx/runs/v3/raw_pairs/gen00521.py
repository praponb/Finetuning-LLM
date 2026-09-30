from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_payments.alias("p"), F.col("o.id") == F.col("p.order_id"), "inner")
    .filter(~F.col("p.method").isin(["bank_transfer", "promptpay", "paypal"]))
    .select(
        F.col("o.amount"),
        F.col("o.channel"),
        F.col("o.order_date"),
        F.col("p.paid_at"),
        F.col("p.status"),
        F.col("p.order_id"),
    )
)
