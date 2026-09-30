from pyspark.sql import functions as F

result = (
    df_orders
    .select("id", "amount", "shipping_fee", "status")
    .orderBy("amount")
)
