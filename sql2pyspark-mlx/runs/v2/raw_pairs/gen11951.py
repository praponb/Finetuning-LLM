from pyspark.sql import functions as F

max_order_date = df_orders.filter(F.col("customer_id").isNotNull()).agg(F.max("order_date")).first()[0]

result = (
    df_orders
    .filter(F.col("order_date") == max_order_date)
    .select("id", "order_date")
    .orderBy("order_date", F.col("id").desc())
    .limit(10)
)
