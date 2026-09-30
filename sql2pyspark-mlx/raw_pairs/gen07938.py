from pyspark.sql import functions as F

result = (
    df_shipments
    .alias("a")
    .join(df_orders.alias("o"), F.col("a.order_id") == F.col("o.id"), "left")
    .groupingSets(
        [["order_status", "carrier"], ["order_status"], []],
        F.col("o.status").alias("order_status"),
        F.col("a.carrier").alias("carrier"),
    )
    .agg(
        F.count("*").alias("rows_total"),
        F.avg(F.col("a.cost")).alias("avg_cost"),
        F.grouping_id().alias("grouping_level"),
    )
)
