from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("channel").orderBy("shipping_fee", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("status").orderBy("shipping_fee", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.shipped_date").desc(), F.col("c.id").desc())

valid = (
    df_orders
    .filter((F.col("order_date") >= F.lit("2023-08-23").cast("date")) & (F.col("channel").eqNullSafe(F.lit("store"))))
    .select(
        "id",
        "customer_id",
        "store_id",
        "status",
        "channel",
        "discount",
        "shipping_fee",
        (F.col("discount") - F.col("shipping_fee")).alias("discount_minus_shipping_fee"),
    )
)

with_windows = (
    valid
    .filter(F.col("store_id") == 16)
    .select(
        "*",
        F.avg("discount_minus_shipping_fee").over(w).alias("running_avg_discount_minus_shipping_fee"),
        F.count("discount_minus_shipping_fee").over(w).alias("running_count_discount_minus_shipping_fee"),
        F.min("discount_minus_shipping_fee").over(w).alias("running_min_discount_minus_shipping_fee"),
        F.first("channel", ignorenulls=True).over(w2).alias("first_known_channel"),
    )
)

enriched = (
    with_windows
    .alias("a")
    .join(df_stores.alias("s"), F.col("a.store_id") == F.col("s.id"), "left")
    .select(
        "a.id",
        "a.customer_id",
        "a.store_id",
        "a.status",
        "a.channel",
        "a.discount",
        "a.shipping_fee",
        "a.running_avg_discount_minus_shipping_fee",
        "a.running_min_discount_minus_shipping_fee",
        "a.first_known_channel",
        F.col("s.sqft"),
    )
)

enriched_orders = (
    enriched
    .alias("e")
    .join(df_customers.alias("d"), F.col("e.customer_id") == F.col("d.id"), "inner")
    .select("e.*", F.col("d.segment"))
)

with_last = (
    enriched_orders
    .alias("s")
    .join(df_shipments.alias("c"), (F.col("c.order_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w3))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.status").alias("shipment_status"), F.col("c.shipped_date"))
)

result = (
    with_last
    .select(
        "id",
        "customer_id",
        "store_id",
        "status",
        "channel",
        "discount",
        "shipping_fee",
        "running_avg_discount_minus_shipping_fee",
        "first_known_channel",
    )
    .orderBy(F.col("status").asc_nulls_last())
)
