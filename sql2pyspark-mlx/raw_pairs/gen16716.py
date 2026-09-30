from pyspark.sql import functions as F

result = (
    df_shipments.alias("shipments")
    .join(df_orders.alias("orders"), F.col("shipments.order_id") == F.col("orders.id"), "inner")
    .filter(~F.col("orders.channel").isin(["web", "mobile"]) | (F.col("orders.amount") <= 1450))
    .select(
        F.col("shipments.order_id"),
        F.col("shipments.carrier"),
        F.col("orders.store_id"),
    )
)
