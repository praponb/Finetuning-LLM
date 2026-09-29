from pyspark.sql import functions as F

cost_by_order_id = df_shipments.groupBy("order_id").agg(F.max("cost").alias("max_shipment_cost"))

with_dims = (
    df_shipments
    .alias("src")
    .join(df_orders.alias("b"), F.col("src.order_id") == F.col("b.id"), "left")
    .select(
        "src.id",
        "src.order_id",
        "src.carrier",
        "src.shipped_date",
        "src.status",
        F.col("b.status").alias("order_status"),
    )
)

result = (
    with_dims
    .join(cost_by_order_id, on="order_id", how="left")
    .select(
        "id",
        "order_id",
        "shipped_date",
        "status",
        "order_status",
        F.col("max_shipment_cost"),
    )
)
