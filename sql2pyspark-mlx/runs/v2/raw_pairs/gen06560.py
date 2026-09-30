from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("status") == "SHIPPED")
    .select("status")
    .unionByName(df_shipments.select("status"))
)
