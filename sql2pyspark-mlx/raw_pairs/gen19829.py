from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id").orderBy("discount", "id")
w2 = Window.orderBy(F.col("shipping_fee").desc(), "id")
w3 = Window.partitionBy("store_id")
w4 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.amount").desc(), F.col("c.id").desc())

valid = (
    df_orders
    .filter(F.col("channel") != "partner")
    .select(
        "id",
        "customer_id",
        "store_id",
        "order_date",
        "status",
        "channel",
        "amount",
        "discount",
        "shipping_fee",
        F.when(F.col("status").isin(["COMPLETED", "SHIPPED"]), "Tier 1").otherwise("Other").alias("status_group"),
    )
)

metrics = (
    valid
    .filter(F.col("order_date") <= "2024-10-29")
    .withColumn("discount_bucket", F.ntile(4).over(w))
    .withColumn("shipping_fee_bucket", F.ntile(10).over(w2))
    .withColumn("corr_shipping_fee_discount", F.corr("shipping_fee", "discount").over(w3))
)

kept = (
    metrics
    .alias("s")
    .join(df_payments.alias("ch"), (F.col("ch.order_id") == F.col("s.id")) & (F.col("ch.paid_at") < F.col("s.order_date")), "left_semi")
)

with_last = (
    kept
    .alias("s")
    .join(df_payments.alias("c"), (F.col("c.order_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w4))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.store_id",
        "s.order_date",
        "s.amount",
        "s.shipping_fee",
        "s.shipping_fee_bucket",
        "s.corr_shipping_fee_discount",
        F.col("c.amount").alias("payment_amount"),
    )
)

not_flagged = (
    with_last
    .alias("s")
    .join(df_shipments.alias("x"), (F.col("x.order_id") == F.col("s.id")) & (F.col("x.cost").between(26, 94)), "left_anti")
)

result = (
    not_flagged
    .groupBy("store_id", F.date_trunc("MONTH", "order_date").alias("activity_month"))
    .agg(
        F.percentile_approx("shipping_fee_bucket", [0.25, 0.5, 0.75]).alias("shipping_fee_bucket_quartiles"),
        F.round(F.avg("shipping_fee"), 2).alias("avg_shipping_fee"),
        F.countDistinct("id").alias("distinct_ids"),
        F.avg("shipping_fee_bucket").alias("avg_shipping_fee_bucket"),
        F.percentile_approx("payment_amount", [0.25, 0.5, 0.75]).alias("payment_amount_quartiles"),
    )
    .orderBy(F.col("avg_shipping_fee_bucket").asc_nulls_last())
)
