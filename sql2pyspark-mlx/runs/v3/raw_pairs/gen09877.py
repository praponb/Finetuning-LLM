from pyspark.sql import functions as F

excluded_orders = df_orders.filter(F.col("order_date").between("2023-04-01", "2025-06-01")).select("id")

result = (
    df_shipments.alias("s")
    .join(excluded_orders.alias("o"), F.col("s.order_id") == F.col("o.id"), "left_anti")
    .filter(F.col("s.order_id").isNotNull() | F.lit(excluded_orders.isEmpty()))
    .filter(F.col("s.delivered_date") < "2023-09-01")
    .select(
        F.col("s.id"),
        F.col("s.order_id"),
        F.col("s.delivered_date"),
        F.col("s.cost"),
    )
)
