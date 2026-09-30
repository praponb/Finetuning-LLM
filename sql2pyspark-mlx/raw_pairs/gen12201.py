from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("status") == "IN_TRANSIT")
    .select("cost")
    .intersect(df_orders.select("shipping_fee"))
    .orderBy("cost")
)
