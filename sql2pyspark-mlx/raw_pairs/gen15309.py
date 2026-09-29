from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.cost").desc(), F.col("c.id").desc())
w2 = Window.partitionBy(F.date_trunc("MONTH", "order_date"), "status").orderBy("order_date", "id").rowsBetween(-6, Window.currentRow)
w3 = Window.partitionBy("status")

filtered = (
    df_orders
    .filter(F.col("order_date") < "2024-05-01")
    .select(
        "id",
        "customer_id",
        "store_id",
        "order_date",
        "status",
        "shipping_fee",
        F.round("amount", 2).alias("amount_rounded"),
        F.coalesce("channel", F.lit("DIRECT")).alias("resolved_channel"),
    )
)

with_last = (
    filtered
    .alias("s")
    .join(df_shipments.alias("c"), (F.col("c.order_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.store_id",
        "s.order_date",
        "s.status",
        "s.shipping_fee",
        "s.amount_rounded",
        "s.resolved_channel",
        F.col("c.status").alias("shipment_status"),
        F.col("c.carrier").alias("shipment_carrier"),
    )
)

with_windows = (
    with_last
    .filter(F.col("status") == "PENDING")
    .withColumn("shipping_fee_avg_6", F.avg("shipping_fee").over(w2))
    .withColumn("status_min_amount_rounded", F.min("amount_rounded").over(w3))
    .withColumn("status_min_shipping_fee", F.min("shipping_fee").over(w3))
)

valid = (
    with_windows
    .filter(F.col("status") != "RETURNED")
    .withColumn("status_min_shipping_fee_minus_status_min_amount_rounded", (F.col("status_min_shipping_fee") - F.col("status_min_amount_rounded")))
)

result = (
    valid
    .groupBy("status", "shipment_carrier")
    .pivot("shipment_status", ["LOST", "RETURNED", "DELIVERED"])
    .agg(F.sum("shipping_fee"))
)
