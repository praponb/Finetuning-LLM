from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("discount") < 0.31)
    .select("id", "shipping_fee")
)
