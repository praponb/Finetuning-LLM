from pyspark.sql import functions as F

result = (
    df_shipments.alias("s")
    .join(df_orders.filter(F.col("order_date").between("2024-07-24", "2024-08-04")).alias("o"), F.col("s.order_id") == F.col("o.id"), "left_semi")
    .filter(F.month(F.col("s.shipped_date")).isin([2, 10, 11]))
    .select(F.col("s.id"), F.col("s.shipped_date"), F.col("s.cost"), F.col("s.status"))
)
