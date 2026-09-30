from pyspark.sql import functions as F

result = (
    df_shipments.alias("s")
    .join(df_orders.alias("o"), F.col("s.order_id") == F.col("o.id"), "inner")
    .filter(F.col("s.cost") <= 53)
    .select(
        F.col("s.carrier"),
        F.col("s.shipped_date"),
        F.col("o.amount"),
        F.col("o.channel"),
    )
)
