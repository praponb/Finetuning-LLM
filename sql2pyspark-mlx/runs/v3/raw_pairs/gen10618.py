from pyspark.sql import functions as F

result = (
    df_shipments.alias("shipments")
    .join(df_orders.alias("orders"), F.col("orders.id") == F.col("shipments.order_id"), "inner")
    .select(
        F.col("shipments.cost"),
        F.col("shipments.carrier"),
        F.col("orders.channel"),
    )
)
