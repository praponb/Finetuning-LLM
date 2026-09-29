from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("shipping_fee") <= 35)
    .select("id", "customer_id", "order_date", "shipping_fee")
)
