from pyspark.sql import functions as F

result = (
    df_shipments.alias("shipments")
    .join(df_orders.alias("orders"), F.col("orders.id") == F.col("shipments.order_id"), "inner")
    .groupBy(F.col("orders.status"))
    .agg(
        F.sum(F.when(F.col("shipments.carrier") == "UPS", 1).otherwise(0)).alias("ups_count"),
    )
    .orderBy(F.col("ups_count").desc(), F.col("status").desc())
)
