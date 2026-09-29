from pyspark.sql import functions as F

result = (
    df_orders
    .filter(((F.col("status") == "PENDING") & (F.col("store_id") == 18)) | F.col("channel").isin(["mobile", "store", "partner"]))
    .select("id", "channel", "shipping_fee", "amount")
)
