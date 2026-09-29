from pyspark.sql import functions as F

result = (
    df_shipments
    .alias("src")
    .join(df_orders.alias("o"), F.col("src.order_id") == F.col("o.id"), "left")
    .groupingSets(
        [["channel", "carrier"], ["channel"]],
        F.col("o.channel").alias("channel"),
        F.col("src.carrier").alias("carrier"),
    )
    .agg(
        F.count("*").alias("cnt"),
        F.max(F.col("src.cost")).alias("max_cost"),
    )
    .orderBy(F.col("channel").asc_nulls_first())
)
