from pyspark.sql import functions as F

result = (
    df_payments
    .filter((F.col("order_id") == 8) & ((F.col("order_id") == 9) | (F.col("status") == "FAILED")))
    .select("order_id")
)
