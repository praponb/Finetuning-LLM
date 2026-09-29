from pyspark.sql import functions as F

result = (
    df_shipments.alias("shipments")
    .join(df_orders.filter(F.col("status").isin(["PENDING", "CANCELLED", "SHIPPED"])).alias("orders"), F.col("shipments.order_id") == F.col("orders.id"), "left_semi")
    .select(F.col("shipments.id"), F.col("shipments.shipped_date"))
    .orderBy("id", F.col("shipped_date").desc())
)
