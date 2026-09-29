from pyspark.sql import functions as F

max_amount = df_orders.filter(F.col("customer_id").isin([35, 38, 40])).agg(F.max("amount")).first()[0]

result = (
    df_orders
    .filter(F.col("amount") >= max_amount)
    .select("id", "amount", "order_date", "customer_id")
    .orderBy("order_date", "id")
    .limit(3)
)
