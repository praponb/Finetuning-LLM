from pyspark.sql import functions as F

excluded_orders = df_orders.filter(F.col("customer_id").isin([1, 15, 26, 27])).select("id")

result = (
    df_shipments.alias("s")
    .join(excluded_orders.alias("o"), F.col("s.order_id") == F.col("o.id"), "left_anti")
    .filter(F.col("s.order_id").isNotNull() | F.lit(excluded_orders.isEmpty()))
    .filter(F.col("s.shipped_date") <= "2025-09-29")
    .select(F.col("s.id"), F.col("s.status"), F.col("s.delivered_date"))
    .orderBy(F.col("status").desc(), "id")
    .limit(10)
)
