from pyspark.sql import functions as F

result = (
    df_shipments.alias("s")
    .join(df_orders.alias("o"), F.col("o.id") == F.col("s.order_id"), "inner")
    .filter((F.col("o.amount") > 1160) & (F.col("o.amount") > 1070))
    .select(
        F.col("s.delivered_date"),
        F.col("s.status"),
        F.col("s.shipped_date"),
        F.col("o.amount"),
    )
    .orderBy("shipped_date")
)
