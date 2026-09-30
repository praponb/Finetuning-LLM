from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("status") == "PENDING")
    .select("status")
    .unionByName(df_shipments.select("status"))
    .distinct()
    .orderBy(F.col("status").desc())
)
