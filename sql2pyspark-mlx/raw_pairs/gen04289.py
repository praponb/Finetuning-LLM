from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.col("method").isin(["card", "paypal", "cash"]) & (F.to_date("paid_at") < "2024-08-23"))
)
