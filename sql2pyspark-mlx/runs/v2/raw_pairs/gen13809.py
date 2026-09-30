from pyspark.sql import functions as F

result = (
    df_shipments.alias("s")
    .join(df_orders.alias("o"), F.col("s.order_id") == F.col("o.id"), "inner")
    .filter((F.col("s.cost") > 33) | F.col("o.channel").isin(["mobile", "store", "partner"]))
    .select(
        F.col("s.id"),
        F.col("s.delivered_date"),
        F.col("s.carrier"),
        F.col("s.order_id"),
        F.col("o.discount"),
        F.concat(F.col("o.channel"), F.lit(" "), F.col("o.status")).alias("channel_status"),
    )
    .orderBy(F.col("carrier").desc(), "delivered_date")
)
