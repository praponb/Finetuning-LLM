from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("status").isin(["COMPLETED", "SHIPPED", "CANCELLED"]) & (F.col("amount") >= 1010))
    .select("id", "store_id", "shipping_fee")
)
