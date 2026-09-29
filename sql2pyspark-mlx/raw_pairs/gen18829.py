from pyspark.sql import functions as F

valid = (
    df_shipments
    .filter(F.col("delivered_date") > "2023-07-18")
    .select(
        "id",
        "order_id",
        "carrier",
        "delivered_date",
        "cost",
        "status",
        F.col("cost").cast("int").alias("cost_int"),
    )
)

result = (
    valid
    .alias("s")
    .join(df_orders.alias("o"), F.col("s.order_id") == F.col("o.id"), "left")
    .groupingSets(
        [["order_status", "shipment_status"], ["order_status"], []],
        F.col("o.status").alias("order_status"),
        F.col("s.status").alias("shipment_status"),
    )
    .agg(
        F.count("*").alias("cnt"),
        F.max(F.col("s.cost_int")).alias("max_cost_int"),
        F.round(F.avg(F.col("s.cost")), 2).alias("avg_cost"),
        F.grouping_id().alias("level_id"),
    )
    .orderBy(F.col("avg_cost").desc_nulls_last())
)
