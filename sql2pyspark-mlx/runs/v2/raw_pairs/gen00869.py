from pyspark.sql import functions as F

result = (
    df_shipments.alias("s")
    .join(df_orders.alias("o"), F.col("s.order_id") == F.col("o.id"), "left")
    .filter(F.col("o.channel").isin(["mobile", "web", "partner"]) & F.col("s.order_id").isNotNull())
    .select(
        F.col("s.id"),
        F.col("s.order_id"),
        F.col("s.shipped_date"),
        F.col("s.cost"),
        F.col("o.status"),
        F.col("o.order_date"),
        F.col("o.store_id"),
    )
    .orderBy("shipped_date", F.col("store_id").desc())
)
