from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.col("order_id") == 32)
    .select("amount", "status", "method")
)
