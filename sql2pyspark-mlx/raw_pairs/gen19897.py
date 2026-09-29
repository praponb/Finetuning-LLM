from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_recent = Window.partitionBy("store_id", "channel").orderBy("shipping_fee", "id")
w2 = Window.partitionBy(F.date_trunc("MONTH", "order_date"), "customer_id").orderBy("shipping_fee", "id")
w3 = Window.partitionBy("channel", "customer_id").orderBy("order_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("status", "customer_id").orderBy("amount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.delivered_date").desc(), F.col("c.id").desc())
w6 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.quantity").desc(), F.col("c.id").desc())

enriched = (
    df_orders
    .withColumn("row_seq", F.row_number().over(w_recent))
    .withColumn("next_amount", F.lead("amount", 2).over(w2))
    .withColumn("first_known_shipping_fee", F.first("shipping_fee", ignorenulls=True).over(w3))
    .withColumn("first_known_amount", F.first("amount", ignorenulls=True).over(w4))
)

with_latest = (
    enriched
    .alias("s")
    .join(df_shipments.alias("c"), (F.col("c.order_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w5))
    .filter(F.col("rn") == 1)
    .select(
        "s.*",
        F.col("c.cost").alias("shipment_cost"),
        F.col("c.shipped_date").alias("shipment_shipped_date"),
    )
)

eligible = (
    with_latest
    .alias("s")
    .join(df_payments.alias("c"), (F.col("c.order_id") == F.col("s.id")) & (F.col("c.paid_at") < F.col("s.order_date")), "left_semi")
)

with_latest_orders = (
    eligible
    .alias("s")
    .join(df_order_items.alias("c"), (F.col("c.order_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w6))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.order_date",
        "s.discount",
        "s.row_seq",
        "s.first_known_amount",
        F.col("c.quantity").alias("order_item_quantity"),
        F.col("c.unit_price"),
    )
)

kept_keys = (
    with_latest_orders
    .select("id")
    .subtract(
        df_orders
        .filter(F.col("status") == "PENDING")
        .select("id")
    )
)

kept_rows = (
    with_latest_orders
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

result = (
    kept_rows
    .groupBy(
        F.when(F.col("first_known_amount") >= 1620, "HIGH").otherwise("LOW").alias("first_known_amount_tier"),
    )
    .agg(
        F.countDistinct("id").alias("distinct_ids"),
        F.countDistinct("id").alias("distinct_ids_2"),
        F.round(F.avg("row_seq"), 2).alias("avg_row_seq"),
        F.count("*").alias("cnt"),
        F.countDistinct("id").alias("distinct_ids_3"),
        F.round(F.avg("discount"), 2).alias("avg_discount"),
    )
    .filter(F.col("distinct_ids") >= 1)
    .orderBy(F.col("cnt").desc())
)
