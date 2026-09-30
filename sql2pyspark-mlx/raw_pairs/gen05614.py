from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("shipping_fee").between(11, 22))
    .select("order_date", "amount")
)
