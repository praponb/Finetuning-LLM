from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_running = Window.partitionBy("carrier", "order_id").orderBy("cost", "id")

valid = (
    df_shipments
    .filter(F.col("shipped_date") >= F.lit("2023-04-21").cast("date"))
    .withColumn("cost_filled", F.coalesce("cost", F.lit(0)))
    .withColumn("resolved_carrier", F.coalesce("carrier", F.lit("UNKNOWN")))
)

enriched = (
    valid
    .filter(F.col("carrier") == "Kerry")
    .select(
        "*",
        F.lead("cost").over(w_running).alias("next_cost"),
        F.lead("cost_filled").over(w_running).alias("next_cost_filled"),
        F.lag("cost_filled").over(w_running).alias("prev_cost_filled"),
        F.row_number().over(w_running).alias("seq_no"),
    )
)

joined = (
    enriched
    .alias("a")
    .join(df_orders.alias("d"), F.col("a.order_id") == F.col("d.id"), "inner")
    .select(
        "a.id",
        "a.order_id",
        "a.shipped_date",
        "a.delivered_date",
        "a.cost",
        "a.resolved_carrier",
        "a.next_cost",
        "a.next_cost_filled",
        "a.seq_no",
        F.col("d.order_date"),
        F.col("d.status").alias("order_status"),
        F.col("d.shipping_fee"),
    )
)

valid_shipments = (
    joined
    .filter(~F.col("order_status").eqNullSafe(F.lit("PENDING")))
    .select(
        "id",
        "shipped_date",
        "delivered_date",
        "next_cost_filled",
        "seq_no",
        "order_date",
        "shipping_fee",
        F.date_sub("shipped_date", 44).alias("shipped_date_minus_44d"),
        F.coalesce("order_status", F.lit("UNKNOWN")).alias("order_status_filled"),
    )
)

cleaned = (
    valid_shipments
    .filter(F.col("next_cost_filled") > 33)
    .select(
        "id",
        "shipped_date",
        "delivered_date",
        "next_cost_filled",
        "seq_no",
        "order_date",
        "shipping_fee",
        "order_status_filled",
        F.round((F.datediff("delivered_date", "order_date") / 30), 1).alias("months_order_date_to_delivered_date"),
    )
)

result = (
    cleaned
    .select(
        "id",
        "shipped_date",
        "next_cost_filled",
        "order_date",
        "shipping_fee",
        "order_status_filled",
        "months_order_date_to_delivered_date",
    )
)
