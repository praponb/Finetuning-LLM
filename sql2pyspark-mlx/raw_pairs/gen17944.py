from pyspark.sql import functions as F

result = (
    df_shipments.alias("shipments")
    .join(df_orders.alias("orders"), F.col("orders.id") == F.col("shipments.order_id"), "left")
    .filter(F.col("shipments.status") == "RETURNED")
    .groupBy(F.col("orders.status"), F.col("orders.channel"))
    .agg(
        F.sum(F.when(F.col("shipments.carrier") == "Flash", F.col("shipments.cost")).otherwise(0)).alias("flash_cost"),
    )
    .orderBy("channel")
)
