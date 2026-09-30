from pyspark.sql import functions as F

result = (
    df_shipments.alias("s")
    .join(df_orders.alias("o"), F.col("s.order_id") == F.col("o.id"), "inner")
    .filter(F.col("s.cost").between(42, 58))
    .select(F.col("s.id"), F.col("s.status"), F.col("o.discount"))
)
