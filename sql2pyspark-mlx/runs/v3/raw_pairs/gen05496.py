from pyspark.sql import functions as F

result = (
    df_orders
    .filter(~F.col("status").isin(["SHIPPED", "CANCELLED"]) & F.col("store_id").isNotNull())
    .select("order_date", "shipping_fee")
)
