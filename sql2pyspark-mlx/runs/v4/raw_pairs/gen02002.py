from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("amount") <= 1040)
    .select("shipping_fee")
    .orderBy("shipping_fee")
)
