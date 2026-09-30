from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.col("method") == "card")
    .select("id", "method", "order_id")
)
