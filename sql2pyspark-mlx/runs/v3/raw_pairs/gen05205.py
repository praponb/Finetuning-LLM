from pyspark.sql import functions as F

result = (
    df_payments
    .filter(~F.col("status").isin(["FAILED", "SUCCESS"]) & (F.col("method").isin(["promptpay", "cash", "paypal"]) | ~F.col("status").isin(["FAILED", "SUCCESS"])))
    .select("id", "paid_at", "amount", F.round("amount", 0).alias("amount_rounded"))
)
