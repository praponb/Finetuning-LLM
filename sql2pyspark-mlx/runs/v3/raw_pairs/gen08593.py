from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.year("order_date") >= 2025)
    .select("id", "order_date", "customer_id", "shipping_fee")
)
