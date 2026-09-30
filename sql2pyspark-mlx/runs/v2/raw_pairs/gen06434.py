from pyspark.sql import functions as F

max_amount = df_orders.agg(F.max("amount")).first()[0]

result = (
    df_orders
    .filter(F.col("amount") >= max_amount)
    .select("id", "amount", "order_date", "status")
)
