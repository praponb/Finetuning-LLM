from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("shipping_fee") < 33)
    .select("id", "amount", "shipping_fee")
)
