from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("discount").isNull())
    .select("id", "discount", "order_date", "shipping_fee")
)
