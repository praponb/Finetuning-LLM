from pyspark.sql import functions as F

excluded_orders = df_orders.filter(F.year("order_date") >= 2025).select("id")

result = (
    df_shipments.alias("shipments")
    .join(excluded_orders.alias("orders"), F.col("shipments.order_id") == F.col("orders.id"), "left_anti")
    .filter(F.col("shipments.order_id").isNotNull() | F.lit(excluded_orders.isEmpty()))
    .filter(F.col("shipments.carrier").isin(["DHL", "Kerry"]))
    .select(F.col("shipments.id"), F.col("shipments.carrier"))
    .orderBy("carrier")
)
