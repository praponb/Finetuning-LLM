from pyspark.sql import functions as F

result = (
    df_shipments.alias("s")
    .join(df_orders.filter(F.col("channel").isin(["store", "web", "partner"])).alias("o"), F.col("s.order_id") == F.col("o.id"), "left_semi")
    .filter(F.col("s.cost") > 74)
    .select(
        F.col("s.id"),
        F.col("s.delivered_date"),
        F.col("s.status"),
        F.col("s.carrier"),
    )
)
