from pyspark.sql import functions as F

result = (
    df_orders
    .filter((F.col("shipping_fee") < 12) | F.col("status").isin(["RETURNED", "COMPLETED", "PENDING"]))
    .select("customer_id")
    .orderBy(F.col("customer_id").desc())
)
