from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("order_id").isin([3, 10, 30, 31]))
    .select("status", "order_id")
    .unionByName(df_shipments.filter(F.col("cost") >= 46).select("status", "order_id"))
    .distinct()
)
