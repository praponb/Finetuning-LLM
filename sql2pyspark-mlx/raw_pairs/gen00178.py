from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.col("method") != "card")
    .select("id", "amount", "method", "order_id")
)
