from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.quantity").desc(), F.col("c.id").desc())
w2 = Window.partitionBy("channel").orderBy("order_item_unit_price", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("channel").orderBy("discount", "id")
w4 = Window.partitionBy("country", "customer_id")
w5 = Window.partitionBy("customer_id", "channel").orderBy(F.col("amount").desc_nulls_first(), "id")
w6 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.shipped_date").desc(), F.col("c.id").desc())
discount_by_customer_id = df_orders.groupBy("customer_id").agg(F.avg("discount").alias("avg_order_discount"))

filtered = (
    df_orders
    .filter((F.col("order_date") > F.to_date(F.lit("2023-06-09"))) & (F.col("channel").eqNullSafe(F.lit("store"))))
    .select(
        "*",
        F.initcap(F.trim("status")).alias("status_title"),
        F.trim("channel").alias("clean_channel"),
    )
)

with_last = (
    filtered
    .alias("s")
    .join(df_order_items.alias("c"), (F.col("c.order_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.customer_id",
        "s.channel",
        "s.amount",
        "s.discount",
        "s.shipping_fee",
        "s.clean_channel",
        F.col("c.unit_price").alias("order_item_unit_price"),
    )
)

with_dims = (
    with_last
    .alias("s")
    .join(df_customers.alias("d"), F.col("s.customer_id") == F.col("d.id"), "left")
    .select("s.*", F.col("d.country"), F.col("d.name"))
)

enriched = (
    with_dims
    .select(
        "id",
        "customer_id",
        "channel",
        "amount",
        "clean_channel",
        "name",
        F.sum("shipping_fee").over(w2).alias("running_shipping_fee"),
        F.lag("discount", 1).over(w3).alias("prev_discount"),
        F.min("shipping_fee").over(w4).alias("country_min_shipping_fee"),
    )
)

deduped = (
    enriched
    .withColumn("dedup_rank", F.row_number().over(w5))
    .filter(F.col("dedup_rank") == 1)
)

with_latest = (
    deduped
    .alias("s")
    .join(df_shipments.alias("c"), (F.col("c.order_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w6))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.carrier"))
)

eligible = (
    with_latest
    .alias("s")
    .join(df_payments.alias("x"), (F.col("x.order_id") == F.col("s.id")) & (F.col("x.order_id").isin([12, 29])), "left_anti")
)

result = (
    eligible
    .join(discount_by_customer_id, on="customer_id", how="left")
    .select(
        "id",
        "customer_id",
        "channel",
        "amount",
        "name",
        "running_shipping_fee",
        "country_min_shipping_fee",
        "dedup_rank",
        "carrier",
        F.when(F.col("channel").rlike("^(partner|mobile)$"), "EXTENDED").when(F.col("channel").rlike("^(web|store)$"), "PRIORITY").otherwise("CORE").alias("channel_zone"),
        F.col("avg_order_discount"),
    )
    .orderBy(F.col("avg_order_discount").asc_nulls_first())
)
