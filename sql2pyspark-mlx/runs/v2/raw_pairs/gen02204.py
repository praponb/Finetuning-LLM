from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_shipments.alias("s"), F.col("o.id") == F.col("s.order_id"), "inner")
    .groupBy(F.col("o.id"))
    .agg(
        F.count(F.col("s.id")).alias("shipment_count"),
        F.count(F.when(F.col("s.carrier") == "Kerry", 1)).alias("kerry_count"),
        (F.max(F.col("s.cost")) - F.min(F.col("s.cost"))).alias("cost_range"),
    )
    .filter(F.col("kerry_count") > 1)
)
