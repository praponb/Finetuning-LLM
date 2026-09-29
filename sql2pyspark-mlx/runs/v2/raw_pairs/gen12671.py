from pyspark.sql import functions as F

result = (
    df_orders
    .select("status")
    .unionByName(df_shipments.filter(F.col("order_id") == 40).select("status"))
    .orderBy(F.col("status").desc())
)
