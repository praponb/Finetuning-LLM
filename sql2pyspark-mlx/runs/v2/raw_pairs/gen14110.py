from pyspark.sql import functions as F

result = (
    df_shipments.alias("s")
    .join(df_orders.alias("o"), F.col("o.id") == F.col("s.order_id"), "inner")
    .select(
        F.col("s.id"),
        F.col("s.shipped_date"),
        F.col("s.delivered_date"),
        F.col("s.cost"),
        F.col("o.status"),
        F.col("o.channel"),
    )
    .orderBy("shipped_date", F.col("cost").desc(), "id")
    .limit(10)
)
