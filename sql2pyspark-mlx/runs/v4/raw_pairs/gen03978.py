from pyspark.sql import functions as F

result = (
    df_shipments.alias("s")
    .join(df_orders.alias("o"), F.col("o.id") == F.col("s.order_id"), "inner")
    .filter(F.month(F.col("s.shipped_date")).isin([1, 6, 7]))
    .groupBy(F.col("o.store_id"))
    .agg(F.sum(F.col("s.cost")).alias("total_cost"))
    .orderBy(F.col("total_cost").desc())
)
