from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.col("order_id") == 21)
    .select("amount", "order_id", "method")
)
