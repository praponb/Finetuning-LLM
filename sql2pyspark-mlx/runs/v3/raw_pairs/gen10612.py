from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("store_id").isNotNull())
    .select("status")
    .subtract(df_orders.filter(F.col("shipping_fee") >= 26).select("status"))
    .orderBy("status")
)
