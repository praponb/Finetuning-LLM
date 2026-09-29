from pyspark.sql import functions as F

result = (
    df_shipments.alias("s")
    .filter(F.col("s.order_id").isNotNull())
    .join(df_orders.filter(F.col("status").isin(["RETURNED", "CANCELLED", "SHIPPED"])).alias("o"), F.col("s.order_id") == F.col("o.id"), "left_anti")
    .filter(F.col("s.carrier").isin(["FedEx", "DHL", "Flash"]))
    .select(F.col("s.id"), F.col("s.order_id"), F.col("s.shipped_date"))
)
