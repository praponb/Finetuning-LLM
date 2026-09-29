from pyspark.sql import functions as F

excluded_orders = df_orders.filter(F.month("order_date").isin([4, 5, 11])).select("id")

result = (
    df_shipments.alias("s")
    .join(excluded_orders.alias("o"), F.col("s.order_id") == F.col("o.id"), "left_anti")
    .filter(F.col("s.order_id").isNotNull() | F.lit(excluded_orders.isEmpty()))
    .select(F.col("s.id"), F.col("s.carrier"), F.col("s.status"), F.col("s.order_id"))
    .orderBy("order_id")
)
