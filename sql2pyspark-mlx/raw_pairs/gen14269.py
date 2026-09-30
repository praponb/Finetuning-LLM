from pyspark.sql import functions as F

result = (
    df_shipments
    .alias("src")
    .join(df_orders.alias("o"), F.col("src.order_id") == F.col("o.id"), "inner")
    .groupingSets(
        [["channel", "carrier"], ["channel"], []],
        F.col("o.channel").alias("channel"),
        F.col("src.carrier").alias("carrier"),
    )
    .agg(
        F.count("*").alias("num_rows"),
        F.avg(F.col("src.cost")).alias("avg_cost"),
        F.grouping_id().alias("level_id"),
    )
)
