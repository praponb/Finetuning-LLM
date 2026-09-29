from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.delivered_date").desc(), F.col("c.id").desc())
w_recent = Window.partitionBy("customer_id").orderBy("amount", "id")
w3 = Window.partitionBy(F.col("s.id"), F.col("s.promo_id")).orderBy(F.col("c.paid_at").desc(), F.col("c.id").desc())

eligible = (
    df_orders
    .alias("s")
    .join(df_order_items.alias("c"), (F.col("c.order_id") == F.col("s.id")) & (F.col("c.product_id").isNull()), "left_semi")
)

latest_match = (
    eligible
    .alias("s")
    .join(df_shipments.alias("c"), (F.col("c.order_id") == F.col("s.id")) & (F.col("c.delivered_date") <= F.col("s.order_date")), "inner")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select(
        "s.*",
        F.col("c.status").alias("shipment_status"),
        F.col("c.cost").alias("shipment_cost"),
    )
)

metrics = (
    latest_match
    .withColumn("next_shipment_cost", F.lead("shipment_cost").over(w_recent))
    .withColumn("prev_shipment_cost", F.lag("shipment_cost").over(w_recent))
    .withColumn("prev_shipping_fee", F.lag("shipping_fee").over(w_recent))
    .withColumn("seq_no", F.row_number().over(w_recent))
)

with_window = (
    metrics
    .alias("a")
    .join(df_promotions.alias("pr"), (F.coalesce(F.col("a.channel"), F.lit("web")) == F.col("pr.channel")) & (F.col("a.order_date") >= F.col("pr.starts_on")) & (F.col("a.order_date") < F.col("pr.ends_on") + F.expr("INTERVAL 7 DAYS")), "left")
    .select(
        "a.*",
        F.col("pr.id").alias("promo_id"),
        F.col("pr.promo_code"),
        F.col("pr.discount_pct"),
    )
)

latest_match_orders = (
    with_window
    .alias("s")
    .join(df_payments.alias("c"), (F.col("c.order_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w3))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.customer_id",
        "s.status",
        "s.channel",
        "s.amount",
        "s.discount",
        "s.shipping_fee",
        "s.shipment_status",
        "s.shipment_cost",
        "s.prev_shipment_cost",
        "s.prev_shipping_fee",
        "s.promo_id",
        "s.promo_code",
        "s.discount_pct",
        F.col("c.method").alias("payment_method"),
        F.col("c.status").alias("payment_status"),
    )
)

joined = (
    latest_match_orders
    .alias("e")
    .join(df_customers.alias("d"), F.col("e.customer_id") == F.col("d.id"), "left")
    .filter(F.col("d.signup_date") >= "2024-04-01")
    .select("e.*", F.col("d.country"), F.col("d.age"), F.col("d.name"))
)

joined_orders = (
    joined
    .alias("a")
    .join(df_customers.alias("b"), F.col("a.customer_id") == F.col("b.id"), "left")
    .select(
        "a.*",
        F.col("b.country").alias("customer_country"),
        F.col("b.age").alias("customer_age"),
    )
)

result = (
    joined_orders
    .select(
        "id",
        "status",
        "channel",
        "shipping_fee",
        "shipment_status",
        "prev_shipment_cost",
        "prev_shipping_fee",
        "promo_id",
        "promo_code",
    )
    .orderBy("prev_shipment_cost", "prev_shipping_fee")
)
