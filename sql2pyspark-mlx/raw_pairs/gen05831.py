from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("store_id").isin([35, 36]))
    .select("status")
    .unionByName(df_orders.filter(F.col("status") == "CANCELLED").select("status"))
    .distinct()
)
