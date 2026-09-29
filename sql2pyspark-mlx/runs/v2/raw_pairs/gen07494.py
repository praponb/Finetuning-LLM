from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("shipping_fee").between(23, 39))
    .select("customer_id")
    .unionByName(df_orders.filter(F.col("shipping_fee") >= 21).select("customer_id"))
    .distinct()
)
