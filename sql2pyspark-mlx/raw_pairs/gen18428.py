from pyspark.sql import functions as F

dim_totals = (
    df_shipments
    .alias("a")
    .join(df_orders.alias("o"), F.col("a.order_id") == F.col("o.id"), "left")
    .groupingSets(
        [["order_status", "channel", "carrier"], ["order_status"]],
        F.col("o.status").alias("order_status"),
        F.col("o.channel").alias("channel"),
        F.col("a.carrier").alias("carrier"),
    )
    .agg(
        F.count("*").alias("rows_total"),
        F.min(F.col("a.cost")).alias("min_cost"),
    )
)

result = (
    dim_totals
    .select("order_status", "carrier", "min_cost")
    .orderBy(F.col("order_status").desc(), "min_cost")
)
