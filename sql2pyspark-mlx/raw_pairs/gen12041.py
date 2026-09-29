from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy("amount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("channel").orderBy("shipping_fee", "id").rowsBetween(-6, Window.currentRow)
w3 = Window.partitionBy("status").orderBy(F.col("first_known_discount").asc_nulls_last())
w4 = Window.partitionBy("customer_id").orderBy("shipping_fee_avg_6", "id")
w5 = Window.orderBy("shipping_fee", "id")
w6 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.cost").desc(), F.col("c.id").desc())
w7 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.delivered_date").desc(), F.col("c.id").desc())

enriched = (
    df_orders
    .select(
        "id",
        "customer_id",
        "store_id",
        "status",
        "channel",
        "amount",
        "shipping_fee",
        F.first("discount", ignorenulls=True).over(w).alias("first_known_discount"),
        F.avg("shipping_fee").over(w2).alias("shipping_fee_avg_6"),
    )
)

metrics = (
    enriched
    .filter(F.col("shipping_fee_avg_6") <= 13)
    .select(
        "id",
        "customer_id",
        "status",
        "channel",
        "shipping_fee",
        "first_known_discount",
        F.cume_dist().over(w3).alias("first_known_discount_cume_dist"),
        F.row_number().over(w4).alias("event_seq"),
        F.lag("shipping_fee_avg_6", 2).over(w5).alias("prev_shipping_fee_avg_6"),
    )
)

with_latest = (
    metrics
    .alias("s")
    .join(df_shipments.alias("c"), (F.col("c.order_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w6))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.status",
        "s.shipping_fee",
        "s.first_known_discount_cume_dist",
        "s.event_seq",
        F.col("c.shipped_date").alias("shipment_shipped_date"),
        F.col("c.status").alias("shipment_status"),
    )
)

latest_match = (
    with_latest
    .alias("s")
    .join(df_shipments.alias("c"), (F.col("c.order_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w7))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.status").alias("shipment_status_2"))
)

eligible = (
    latest_match
    .alias("s")
    .join(df_shipments.alias("ch"), (F.col("ch.order_id") == F.col("s.id")) & (F.col("ch.delivered_date") >= F.col("s.shipment_shipped_date")), "left_semi")
)

kept_keys = (
    eligible
    .select("id", "status")
    .intersectAll(
        df_orders
        .filter(F.col("order_date").between("2024-10-01", "2025-10-24"))
        .select("id", "status")
    )
)

verified = (
    eligible
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.status") == F.col("k.status")), "inner")
    .select("s.*")
)

eligible_orders = (
    verified
    .alias("s")
    .join(df_order_items.alias("c"), (F.col("c.order_id") == F.col("s.id")) & (F.col("c.unit_price") < 390), "left_semi")
)

result = (
    eligible_orders
    .select("shipment_status_2", "shipping_fee", "first_known_discount_cume_dist")
    .unpivot(
        ids=["shipment_status_2"],
        values=["shipping_fee", "first_known_discount_cume_dist"],
        variableColumnName="kpi",
        valueColumnName="value",
    )
    .filter(F.col("value").isNotNull())
)
