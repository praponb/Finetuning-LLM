from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status", "order_status").orderBy(F.col("shipping_fee").desc(), "id")
w2 = Window.partitionBy("status", "carrier").orderBy("shipping_fee", "id")
w3 = Window.partitionBy("carrier").orderBy("latest_rank", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

with_dims = (
    df_shipments
    .alias("e")
    .join(df_orders.alias("p"), F.col("e.order_id") == F.col("p.id"), "left")
    .select(
        "e.id",
        "e.order_id",
        "e.carrier",
        "e.cost",
        "e.status",
        F.col("p.status").alias("order_status"),
        F.col("p.channel"),
        F.col("p.shipping_fee"),
    )
)

filtered = (
    with_dims
    .filter(F.col("carrier").isNotNull() & F.col("carrier").isin(["DHL", "Kerry"]))
    .select(
        "id",
        "carrier",
        "status",
        "order_status",
        "channel",
        "shipping_fee",
        F.concat("status", F.lit(" "), "channel").alias("status_channel"),
    )
)

ranked = (
    filtered
    .withColumn("latest_rank", F.row_number().over(w))
    .filter(F.col("latest_rank") == 1)
)

with_windows = (
    ranked
    .filter(~F.col("order_status").isin(["SHIPPED", "CANCELLED", "RETURNED"]))
    .select(
        "*",
        F.lag("shipping_fee").over(w2).alias("prev_shipping_fee"),
        F.row_number().over(w2).alias("seq_no"),
        F.last("order_status", ignorenulls=True).over(w3).alias("last_known_order_status"),
    )
)

valid = (
    with_windows
    .filter(F.col("prev_shipping_fee").isNull())
    .select(
        "id",
        "carrier",
        "status",
        "order_status",
        "channel",
        "status_channel",
        "latest_rank",
        "seq_no",
        "last_known_order_status",
        F.initcap(F.trim("carrier")).alias("carrier_title"),
        (F.col("seq_no") + F.col("prev_shipping_fee")).alias("seq_no_plus_prev_shipping_fee"),
        F.coalesce("order_status", F.lit("N/A")).alias("order_status_clean"),
    )
)

valid_shipments = (
    valid
    .filter((F.col("order_status").eqNullSafe(F.lit("SHIPPED"))) & (F.col("order_status") == "SHIPPED"))
    .withColumn("seq_no_plus_prev_shipping_fee_times_latest_rank", (F.col("seq_no_plus_prev_shipping_fee") * F.col("latest_rank")))
    .withColumn("carrier_filled", F.coalesce("carrier", F.lit("NONE")))
)

result = (
    valid_shipments
    .groupBy(
        F.when(F.col("seq_no_plus_prev_shipping_fee_times_latest_rank") >= 74, "HIGH").otherwise("LOW").alias("seq_no_plus_prev_shipping_fee_times_latest_rank_tier"),
        "status",
    )
    .agg(
        F.count(F.when(F.col("channel") == "web", 1)).alias("web_count"),
        F.sum(F.when(F.col("carrier") == "Kerry", F.col("seq_no_plus_prev_shipping_fee")).otherwise(0)).alias("kerry_seq_no_plus_prev_shipping_fee"),
        F.round(F.sum("seq_no_plus_prev_shipping_fee") / F.nullif(F.count(F.when(F.col("channel") == "partner", 1)), F.lit(0)), 2).alias("seq_no_plus_prev_shipping_fee_per_partner"),
        F.count_if(F.col("carrier") == "UPS").alias("ups_events"),
        F.round(F.avg("seq_no_plus_prev_shipping_fee"), 2).alias("avg_seq_no_plus_prev_shipping_fee"),
        F.avg(F.when(F.col("carrier") == "UPS", F.col("seq_no_plus_prev_shipping_fee_times_latest_rank"))).alias("ups_avg_seq_no_plus_prev_shipping_fee_times_latest_rank"),
    )
)
