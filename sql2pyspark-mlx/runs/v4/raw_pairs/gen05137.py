from pyspark.sql import functions as F

result = (
    df_shipments.alias("s")
    .join(df_orders.alias("o"), F.col("s.order_id") == F.col("o.id"), "inner")
    .filter(F.col("o.status") != "SHIPPED")
    .groupBy(F.col("o.channel"), F.col("o.status"))
    .agg(
        F.sum(F.when(F.col("s.carrier") == "DHL", 1).otherwise(0)).alias("dhl_count"),
        F.count(F.col("s.cost")).alias("cost_count"),
    )
)
