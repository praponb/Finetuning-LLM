from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("amount") > 1510)
    .select("store_id")
    .unionByName(df_orders.filter(F.col("status") == "SHIPPED").select("store_id"))
)
