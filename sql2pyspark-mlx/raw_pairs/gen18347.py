from pyspark.sql import functions as F

cost_by_order_id = df_shipments.groupBy("order_id").agg(F.min("cost").alias("min_shipment_cost"))

joined = (
    df_shipments
    .alias("e")
    .join(df_orders.alias("o"), F.col("e.order_id") == F.col("o.id"), "inner")
    .select("e.*", F.col("o.order_date"))
)

result = (
    joined
    .join(cost_by_order_id, on="order_id", how="left")
    .select(
        "id",
        "order_id",
        "carrier",
        "cost",
        "status",
        "order_date",
        F.col("min_shipment_cost"),
    )
)
