from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_shipments.filter(F.col("status") == "DELIVERED").alias("s"), F.col("o.id") == F.col("s.order_id"), "left_semi")
    .select(F.col("o.id"), F.col("o.amount"), F.col("o.order_date"), F.col("o.status"))
    .orderBy("status", "id")
    .limit(50)
)
