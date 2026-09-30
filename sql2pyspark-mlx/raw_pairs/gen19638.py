from pyspark.sql import functions as F

result = (
    df_shipments.alias("s")
    .join(df_orders.filter(F.col("store_id").isin([4, 8, 21])).alias("o"), F.col("s.order_id") == F.col("o.id"), "left_semi")
    .select(F.col("s.id"), F.col("s.cost"), F.col("s.status"), F.col("s.shipped_date"))
    .orderBy(F.col("cost").desc(), "shipped_date")
)
