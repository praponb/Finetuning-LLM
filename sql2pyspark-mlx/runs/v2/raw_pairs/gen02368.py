from pyspark.sql import functions as F

avg_amount = df_orders.agg(F.avg("amount")).first()[0]

result = (
    df_orders
    .filter(F.col("amount") >= avg_amount)
    .select("id", "amount", "order_date")
    .orderBy(F.col("id").desc())
)
