from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("amount").between(560, 1550))
    .select("id", "order_date", "status", "amount")
)
