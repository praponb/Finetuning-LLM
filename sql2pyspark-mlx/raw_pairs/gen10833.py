from pyspark.sql import functions as F

result = (
    df_shipments.alias("s")
    .join(df_orders.alias("o"), F.col("s.order_id") == F.col("o.id"), "inner")
    .groupBy(F.col("o.channel"))
    .agg(
        F.sum(F.when(F.col("s.carrier") == "DHL", 1).otherwise(0)).alias("dhl_count"),
        (F.max(F.col("s.cost")) - F.min(F.col("s.cost"))).alias("cost_range"),
    )
    .orderBy("channel")
    .limit(20)
)
