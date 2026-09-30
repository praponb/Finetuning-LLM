from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("status").isin(["RETURNED", "LOST", "DELIVERED"]))
    .select("delivered_date")
    .unionByName(df_shipments.filter(F.col("order_id") == 17).select("delivered_date"))
    .orderBy("delivered_date")
)
