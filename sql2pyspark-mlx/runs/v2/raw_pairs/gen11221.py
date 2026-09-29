from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("discount").isNotNull())
    .select("id", "store_id")
    .unionByName(df_orders.filter(F.col("shipping_fee") < 18).select("id", "store_id"))
    .distinct()
)
