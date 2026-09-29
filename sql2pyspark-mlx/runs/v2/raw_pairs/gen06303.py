from pyspark.sql import functions as F

result = (
    df_shipments.alias("s")
    .join(df_orders.filter(F.col("customer_id").isin([5, 33])).alias("o"), F.col("s.order_id") == F.col("o.id"), "left_semi")
    .filter(F.col("s.order_id").isNull())
    .select(F.col("s.id"), F.col("s.status"), F.col("s.carrier"))
    .orderBy("status", "id")
    .limit(5)
)
