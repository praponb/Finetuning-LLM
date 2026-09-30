from pyspark.sql import functions as F

result = (
    df_payments
    .filter(~((F.col("method") == "cash") & ~F.col("method").isin(["card", "promptpay", "cash"])))
    .select("id", "amount", "order_id", "paid_at")
)
