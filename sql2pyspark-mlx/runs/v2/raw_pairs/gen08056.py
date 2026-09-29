from pyspark.sql import functions as F

result = (
    df_orders
    .filter(((F.col("status") == "CANCELLED") & (F.col("shipping_fee") >= 30)) | (F.col("store_id") == 2))
    .select("customer_id", "channel", "order_date")
    .orderBy("order_date")
)
