from pyspark.sql import functions as F

result = (
    df_shipments.alias("shipments")
    .join(df_orders.filter(F.col("channel").isNull()).alias("orders"), F.col("shipments.order_id") == F.col("orders.id"), "left_semi")
    .select(
        F.col("shipments.id"),
        F.col("shipments.shipped_date"),
        F.col("shipments.cost"),
    )
    .orderBy(F.col("id").desc())
    .limit(10)
)
