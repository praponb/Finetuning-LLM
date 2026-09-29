from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("status").isin(["RETURNED", "CANCELLED"]) | F.col("shipping_fee").between(10, 31))
    .select("order_date", F.col("amount").cast("int").alias("amount_int"))
)
