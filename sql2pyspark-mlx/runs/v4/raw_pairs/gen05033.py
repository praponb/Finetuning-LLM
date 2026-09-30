from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.col("amount").between(490, 1420))
    .select("order_id", "amount", "method")
)
