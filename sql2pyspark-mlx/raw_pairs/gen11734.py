from pyspark.sql import functions as F

result = (
    df_shipments
    .alias("a")
    .join(df_orders.alias("dim"), F.col("a.order_id") == F.col("dim.id"), "left")
    .groupingSets(
        [["channel", "order_status", "carrier"], ["channel"]],
        F.col("dim.channel").alias("channel"),
        F.col("dim.status").alias("order_status"),
        F.col("a.carrier").alias("carrier"),
    )
    .agg(
        F.count("*").alias("cnt"),
        F.min(F.col("a.cost")).alias("min_cost"),
        F.grouping_id().alias("grouping_level"),
    )
    .orderBy(F.col("cnt").desc_nulls_last())
)
