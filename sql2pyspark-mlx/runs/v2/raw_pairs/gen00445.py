from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("channel").isin(["mobile", "web", "store"]))
    .select("order_date", "shipping_fee")
)
