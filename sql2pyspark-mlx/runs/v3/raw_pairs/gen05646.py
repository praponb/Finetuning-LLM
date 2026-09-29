from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("store_id") == 4)
    .select("id", "shipping_fee", "customer_id")
    .orderBy(F.col("customer_id").desc(), "id")
)
