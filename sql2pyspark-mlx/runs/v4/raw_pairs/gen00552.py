from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.cost").desc(), F.col("c.id").desc())

cleaned = (
    df_orders
    .filter((F.col("order_date") >= F.lit("2023-07-31").cast("date")) & F.col("channel").isNotNull() & (F.col("status") == "CANCELLED"))
    .select(
        "id",
        "customer_id",
        "store_id",
        "status",
        "amount",
        "discount",
        "shipping_fee",
        F.substring("channel", 1, 2).alias("channel_prefix"),
        F.coalesce("channel", F.lit("UNKNOWN")).alias("resolved_channel"),
    )
)

with_latest = (
    cleaned
    .alias("s")
    .join(df_shipments.alias("c"), (F.col("c.order_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.delivered_date").alias("shipment_delivered_date"))
)

with_last = (
    with_latest
    .alias("s")
    .join(df_shipments.alias("c"), (F.col("c.order_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.delivered_date"))
)

result = (
    with_last
    .groupBy(F.date_trunc("month", "delivered_date").alias("activity_month"))
    .agg(
        F.max(F.when(F.col("status") == "SHIPPED", F.col("discount"))).alias("shipped_max_discount"),
        F.max("shipping_fee").alias("max_shipping_fee"),
        F.count(F.when(F.col("status") == "PENDING", 1)).alias("pending_rows"),
    )
    .filter(F.col("pending_rows") > 1)
)
