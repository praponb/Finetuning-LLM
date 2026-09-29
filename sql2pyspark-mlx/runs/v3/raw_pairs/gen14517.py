from pyspark.sql import functions as F

max_discount = df_orders.filter(F.col("discount") > 0.35).agg(F.max("discount")).first()[0]

result = (
    df_orders
    .filter(F.col("discount") >= max_discount)
    .select("id", "discount", "order_date", "amount")
)
