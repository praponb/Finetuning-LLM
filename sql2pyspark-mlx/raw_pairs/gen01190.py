from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id", "carrier").orderBy(F.col("shipping_fee").desc(), "id")
w2 = Window.partitionBy("order_id")
w3 = Window.partitionBy("order_id").orderBy("is_lost", "id")

cleaned = (
    df_shipments
    .filter(F.col("carrier").isNotNull() & (F.year("delivered_date") == 2025))
    .withColumn("status_prefix", F.substring("status", 1, 3))
    .withColumn("carrier_filled", F.coalesce("carrier", F.lit("N/A")))
)

joined = (
    cleaned
    .alias("a")
    .join(df_orders.alias("b"), F.col("a.order_id") == F.col("b.id"), "left")
    .filter(F.col("b.amount") > 770)
    .select(
        "a.id",
        "a.order_id",
        "a.carrier",
        "a.cost",
        "a.status",
        "a.status_prefix",
        "a.carrier_filled",
        F.col("b.shipping_fee"),
        F.col("b.order_date"),
    )
)

filtered = (
    joined
    .filter(F.col("order_id").isNotNull())
    .withColumn("is_lost", F.when(F.col("status") == "LOST", 1).otherwise(0))
)

valid = (
    filtered
    .filter(~F.col("carrier").eqNullSafe(F.lit("UPS")))
    .withColumn("order_date_label", F.date_format("order_date", "MMM yyyy"))
    .withColumn("carrier_upper", F.upper("carrier"))
)

enriched = (
    valid
    .select(
        "id",
        "order_id",
        "carrier",
        "cost",
        "carrier_filled",
        "shipping_fee",
        "order_date_label",
        "carrier_upper",
        F.ntile(3).over(w).alias("shipping_fee_bucket"),
        F.stddev_samp("is_lost").over(w2).alias("order_id_stddev_samp_is_lost"),
        F.lead("shipping_fee", 1).over(w3).alias("next_shipping_fee"),
    )
)

result = (
    enriched
    .select(
        "id",
        "carrier",
        "cost",
        "order_date_label",
        "shipping_fee_bucket",
        "order_id_stddev_samp_is_lost",
        "next_shipping_fee",
    )
)
