from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("status").isin(["COMPLETED", "SHIPPED", "PENDING"]))
    .select("status", "channel")
    .unionByName(df_orders.filter(F.col("store_id") == 28).select("status", "channel"))
    .distinct()
    .orderBy("status")
)
