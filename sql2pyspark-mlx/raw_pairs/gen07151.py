from pyspark.sql import functions as F

excluded_orders = df_orders.filter(F.col("store_id").isNull()).select("id")

result = (
    df_shipments.alias("s")
    .join(excluded_orders.alias("o"), F.col("s.order_id") == F.col("o.id"), "left_anti")
    .filter(F.col("s.order_id").isNotNull() | F.lit(excluded_orders.isEmpty()))
    .select(F.col("s.id"), F.col("s.cost"), F.col("s.order_id"))
    .orderBy("cost")
)
