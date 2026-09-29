from pyspark.sql import functions as F

result = (
    df_payments
    .filter((F.col("order_id") == 23) & (F.col("amount") >= 1380))
    .select("id", "order_id", "amount")
)
