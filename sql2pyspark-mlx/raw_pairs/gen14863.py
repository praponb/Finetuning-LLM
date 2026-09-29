from pyspark.sql import functions as F

quantity_by_order_id = df_order_items.groupBy("order_id").agg(F.max("quantity").alias("max_order_item_quantity"))

joined = (
    df_shipments
    .alias("src")
    .join(df_orders.alias("d"), F.col("src.order_id") == F.col("d.id"), "left")
    .select("src.*", F.col("d.channel"))
)

result = (
    joined
    .join(quantity_by_order_id, on="order_id", how="left")
    .select(
        "id",
        "order_id",
        "carrier",
        "shipped_date",
        "cost",
        "status",
        "channel",
        F.col("max_order_item_quantity"),
    )
)
