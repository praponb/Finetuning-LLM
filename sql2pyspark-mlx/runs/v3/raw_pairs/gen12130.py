from pyspark.sql import functions as F

result = (
    df_shipments.alias("s")
    .join(df_orders.alias("o"), F.col("s.order_id") == F.col("o.id"), "inner")
    .filter(F.month(F.col("o.order_date")).isin([3, 5, 10]))
    .select(
        F.col("s.carrier"),
        F.col("s.cost"),
        F.col("s.status"),
        F.col("o.order_date"),
    )
)
