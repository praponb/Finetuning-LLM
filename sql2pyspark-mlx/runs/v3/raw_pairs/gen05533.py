from pyspark.sql import functions as F

result = (
    df_shipments.alias("s")
    .join(df_orders.alias("o"), F.col("o.id") == F.col("s.order_id"), "left")
    .groupBy(F.col("o.channel"), F.col("o.status"))
    .agg(
        F.count("*").alias("cnt"),
        F.countDistinct(F.col("s.cost")).alias("unique_carriers"),
    )
    .orderBy("cnt")
)
