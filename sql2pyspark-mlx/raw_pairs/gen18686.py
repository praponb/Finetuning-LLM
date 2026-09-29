from pyspark.sql import functions as F

result = (
    df_shipments
    .alias("src")
    .join(df_orders.alias("d"), F.col("src.order_id") == F.col("d.id"), "left")
    .groupBy(F.col("d.channel").alias("channel"), F.col("src.carrier").alias("carrier"))
    .agg(
        F.count("*").alias("cnt"),
        F.min(F.col("src.cost")).alias("min_cost"),
    )
    .orderBy(F.col("cnt").asc_nulls_first())
)
