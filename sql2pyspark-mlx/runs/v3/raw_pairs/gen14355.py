from pyspark.sql import functions as F

result = (
    df_shipments.alias("s")
    .join(df_orders.alias("o"), F.col("s.order_id") == F.col("o.id"), "outer")
    .filter(F.col("s.carrier").isin(["UPS", "DHL", "Kerry"]) & ~F.col("o.channel").isin(["mobile", "store"]))
    .select(
        F.col("s.order_id"),
        F.col("o.amount"),
        F.col("o.discount"),
        F.col("o.status"),
    )
    .orderBy(F.col("status").desc())
)
