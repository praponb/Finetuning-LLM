from pyspark.sql import functions as F

result = (
    df_payments
    .filter(~F.col("method").isin(["bank_transfer", "card"]))
    .select("status", "order_id")
)
