from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.delivered_date").desc(), F.col("c.id").desc())

with_last = (
    df_orders
    .alias("s")
    .join(df_shipments.alias("c"), (F.col("c.order_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select(
        "s.*",
        F.col("c.shipped_date").alias("shipment_shipped_date"),
        F.col("c.status").alias("shipment_status"),
    )
)

result = (
    with_last
    .groupBy("store_id")
    .agg(
        F.max(F.when(F.col("status") == "CANCELLED", F.col("amount"))).alias("cancelled_max_amount"),
        F.sum(F.when(F.col("status") == "PENDING", F.col("shipping_fee")).otherwise(0)).alias("pending_shipping_fee"),
        F.sum(F.when(F.col("channel") == "web", F.col("shipping_fee")).otherwise(0)).alias("web_shipping_fee"),
    )
)
