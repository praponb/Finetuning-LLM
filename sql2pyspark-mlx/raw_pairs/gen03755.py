from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_recent = Window.partitionBy("customer_id").orderBy("amount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("status", "channel").orderBy(F.col("shipping_fee").desc(), "id")
w_ordered = Window.partitionBy(F.year("signup_date"), "channel").orderBy(F.col("shipping_fee_bucket").desc_nulls_last())
w4 = Window.partitionBy(F.year("signup_date"), "customer_id").orderBy("running_min_discount", "id").rowsBetween(-3, Window.currentRow)
w5 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.unit_price").desc(), F.col("c.id").desc())
w6 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.shipped_date").desc(), F.col("c.id").desc())

scored = (
    df_orders
    .filter(F.col("discount") <= 0.11)
    .select(
        "id",
        "customer_id",
        "order_date",
        "channel",
        "discount",
        F.avg("discount").over(w_recent).alias("running_avg_discount"),
        F.max("discount").over(w_recent).alias("running_max_discount"),
        F.min("discount").over(w_recent).alias("running_min_discount"),
        F.count("discount").over(w_recent).alias("running_count_discount"),
        F.ntile(5).over(w2).alias("shipping_fee_bucket"),
    )
)

filtered = (
    scored
    .filter(F.col("customer_id").isNotNull())
    .select(
        "id",
        "customer_id",
        "channel",
        "discount",
        "running_avg_discount",
        "running_min_discount",
        "running_count_discount",
        "shipping_fee_bucket",
        F.round((F.col("shipping_fee_bucket") * 1.07), 2).alias("shipping_fee_bucket_adjusted"),
    )
)

joined = (
    filtered
    .alias("e")
    .join(df_customers.alias("p"), F.col("e.customer_id") == F.col("p.id"), "left")
    .select(
        "e.id",
        "e.customer_id",
        "e.channel",
        "e.running_min_discount",
        "e.shipping_fee_bucket",
        "e.shipping_fee_bucket_adjusted",
        F.col("p.name"),
        F.col("p.signup_date"),
        F.col("p.age"),
    )
)

metrics = (
    joined
    .select(
        "id",
        "shipping_fee_bucket",
        "shipping_fee_bucket_adjusted",
        "name",
        "signup_date",
        "age",
        F.percent_rank().over(w_ordered).alias("shipping_fee_bucket_percent_rank"),
        F.avg("age").over(w4).alias("age_avg_3"),
    )
)

with_latest = (
    metrics
    .alias("s")
    .join(df_order_items.alias("c"), (F.col("c.order_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w5))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.quantity"))
)

with_last = (
    with_latest
    .alias("s")
    .join(df_shipments.alias("c"), (F.col("c.order_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w6))
    .filter(F.col("rn") == 1)
    .select(
        "s.*",
        F.col("c.carrier").alias("shipment_carrier"),
        F.col("c.shipped_date"),
    )
)

result = (
    with_last
    .select(
        "id",
        "shipping_fee_bucket",
        "shipping_fee_bucket_adjusted",
        "signup_date",
        "age",
        "age_avg_3",
        "shipment_carrier",
        "shipped_date",
        F.when(F.col("shipment_carrier").rlike("^(DHL|Kerry)$"), "GROUP_B").when(F.col("shipment_carrier").rlike("^(FedEx|Flash)$"), "OTHER_ZONE").otherwise("PRIORITY").alias("shipment_carrier_zone"),
    )
    .orderBy(F.col("shipment_carrier").desc(), "age")
)
