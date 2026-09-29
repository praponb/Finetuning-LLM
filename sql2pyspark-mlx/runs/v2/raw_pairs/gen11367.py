from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("shipping_fee") >= 29)
    .select("id", "status")
    .unionByName(df_orders.filter(F.col("store_id").isNotNull()).select("id", "status"))
)
