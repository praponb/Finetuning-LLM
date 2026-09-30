from pyspark.sql import functions as F

result = (
    df_payments
    .filter((F.col("order_id") == 34) & (~F.col("method").isin(["promptpay", "card", "paypal"]) | (F.to_date("paid_at") < "2024-10-21")))
)
