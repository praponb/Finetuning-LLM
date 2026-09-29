from pyspark.sql import functions as F

result = (
    df_orders
    .filter((F.col("shipping_fee") > 30) & ((F.col("amount") >= 870) | F.col("status").isin(["CANCELLED", "SHIPPED"])))
    .select("id", "customer_id", F.length("status").alias("status_length"))
)
