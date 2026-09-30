from pyspark.sql import functions as F

result = (
    df_shipments
    .alias("e")
    .join(df_orders.alias("p"), F.col("e.order_id") == F.col("p.id"), "inner")
    .groupBy(F.col("p.channel").alias("channel"), F.col("e.carrier").alias("carrier"))
    .agg(
        F.count("*").alias("cnt"),
        F.sum(F.col("e.cost")).alias("total_cost"),
    )
)
