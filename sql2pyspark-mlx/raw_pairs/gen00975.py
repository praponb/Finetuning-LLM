from pyspark.sql import functions as F

cost_by_order_id = df_shipments.groupBy("order_id").agg(F.avg("cost").alias("avg_shipment_cost"))

enriched = (
    df_order_items
    .alias("e")
    .join(df_products.alias("b"), F.col("e.product_id").eqNullSafe(F.col("b.id")), "left")
    .select("e.*", F.col("b.launch_date"))
)

result = (
    enriched
    .join(cost_by_order_id, on="order_id", how="left")
    .select(
        "id",
        "order_id",
        "product_id",
        "unit_price",
        "launch_date",
        F.col("avg_shipment_cost"),
    )
)
