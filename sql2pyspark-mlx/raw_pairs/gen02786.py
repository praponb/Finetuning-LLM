from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id")
w2 = Window.partitionBy("status")
w3 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.paid_at").desc(), F.col("c.id").desc())
w4 = Window.partitionBy("store_id", "customer_id").orderBy(F.col("payment_amount").asc_nulls_first(), "id")

valid = (
    df_orders
    .filter((F.col("amount") >= 500) & (F.col("order_date") >= F.to_date(F.lit("2023-04-02"))))
    .withColumn("status_prefix", F.substring("status", 1, 2))
)

not_flagged = (
    valid
    .alias("s")
    .join(df_order_items.alias("ch"), (F.col("ch.order_id") == F.col("s.id")) & (F.col("ch.product_id") == 26), "left_anti")
)

not_flagged_orders = (
    not_flagged
    .alias("s")
    .join(df_shipments.alias("c"), (F.col("c.order_id") == F.col("s.id")) & (F.col("c.delivered_date") <= F.col("s.order_date")), "left_semi")
)

enriched = (
    not_flagged_orders
    .select(
        "*",
        F.corr("discount", "amount").over(w).alias("corr_discount_amount"),
        F.corr("amount", "shipping_fee").over(w2).alias("corr_amount_shipping_fee"),
    )
)

with_latest = (
    enriched
    .alias("s")
    .join(df_payments.alias("c"), (F.col("c.order_id") == F.col("s.id")) & (F.col("c.paid_at") <= F.col("s.order_date")), "inner")
    .withColumn("rn", F.row_number().over(w3))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.amount").alias("payment_amount"))
)

enriched_orders = (
    with_latest
    .alias("e")
    .join(df_customers.alias("d"), F.col("e.customer_id") == F.col("d.id"), "left")
    .select(
        "e.id",
        "e.customer_id",
        "e.store_id",
        "e.amount",
        "e.shipping_fee",
        "e.payment_amount",
        F.col("d.signup_date"),
    )
)

latest = (
    enriched_orders
    .withColumn("rank_in_group", F.row_number().over(w4))
    .filter(F.col("rank_in_group") == 1)
    .drop("rank_in_group")
)

result = (
    latest
    .groupBy(
        F.when(F.col("shipping_fee") >= 33, "PREMIUM").otherwise("STANDARD").alias("shipping_fee_tier"),
        "store_id",
    )
    .agg(
        F.percentile_approx("shipping_fee", 0.9).alias("shipping_fee_p90"),
        F.countDistinct("store_id").alias("distinct_store_ids"),
        F.count("*").alias("cnt"),
    )
    .orderBy(F.col("shipping_fee_p90").desc_nulls_last())
)
