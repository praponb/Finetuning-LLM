from pyspark.sql import functions as F

excluded_orders = df_orders.filter(F.col("status").isin(["RETURNED", "SHIPPED", "COMPLETED"])).select("id")

result = (
    df_shipments.alias("s")
    .join(excluded_orders.alias("o"), F.col("s.order_id") == F.col("o.id"), "left_anti")
    .filter(F.col("s.order_id").isNotNull() | F.lit(excluded_orders.isEmpty()))
    .filter(F.col("s.delivered_date").between("2023-09-22", "2025-06-12"))
    .select(F.col("s.id"), F.col("s.cost"), F.col("s.shipped_date"))
)
