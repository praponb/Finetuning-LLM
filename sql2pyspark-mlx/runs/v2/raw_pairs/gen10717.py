from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("amount") <= 740)
    .select("id", "shipping_fee", "customer_id")
)
