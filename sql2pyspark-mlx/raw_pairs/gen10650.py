from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.date_trunc("YEAR", "order_date"), "order_id").orderBy("status_length", "id").rowsBetween(-3, -1)
w2 = Window.partitionBy("order_id").orderBy("order_date", "id")
w3 = Window.partitionBy("order_id")
w_ordered = Window.partitionBy("order_id").orderBy("shipping_fee", "id")
w5 = Window.partitionBy(F.date_trunc("YEAR", "order_date"), "order_id").orderBy("shipping_fee", "id")
w_part = Window.orderBy("next_order_id_max_status_length", "id")

valid = (
    df_shipments
    .filter((F.col("delivered_date") >= F.lit("2023-07-15").cast("date")) & F.col("status").isin(["LOST", "IN_TRANSIT", "DELIVERED"]))
    .select(
        "id",
        "order_id",
        "shipped_date",
        "cost",
        "status",
        F.length("status").alias("status_length"),
    )
)

enriched = (
    valid
    .alias("s")
    .join(df_orders.alias("d"), F.col("s.order_id").eqNullSafe(F.col("d.id")), "inner")
    .select(
        "s.id",
        "s.order_id",
        "s.status_length",
        F.col("d.channel"),
        F.col("d.order_date"),
        F.col("d.shipping_fee"),
    )
)

with_windows = (
    enriched
    .select(
        "id",
        "order_id",
        "order_date",
        "shipping_fee",
        F.avg("shipping_fee").over(w).alias("shipping_fee_avg_3_prior"),
        F.row_number().over(w2).alias("seq_no"),
        F.max("status_length").over(w3).alias("order_id_max_status_length"),
    )
)

metrics = (
    with_windows
    .select(
        "id",
        "order_date",
        "shipping_fee_avg_3_prior",
        F.row_number().over(w_ordered).alias("event_seq"),
        F.lead("order_id_max_status_length", 2).over(w5).alias("next_order_id_max_status_length"),
        F.lead("order_id_max_status_length", 2).over(w2).alias("next_order_id_max_status_length_2"),
    )
)

with_windows_shipments = (
    metrics
    .select(
        "*",
        F.lead("shipping_fee_avg_3_prior").over(w_part).alias("next_shipping_fee_avg_3_prior"),
        F.lag("shipping_fee_avg_3_prior").over(w_part).alias("prev_shipping_fee_avg_3_prior"),
        F.lag("event_seq").over(w_part).alias("prev_event_seq"),
        F.lead("event_seq").over(w_part).alias("next_event_seq"),
        F.row_number().over(w_part).alias("seq_no"),
    )
)

result = (
    with_windows_shipments
    .select(
        "id",
        "order_date",
        "shipping_fee_avg_3_prior",
        "event_seq",
        "next_order_id_max_status_length",
        "next_order_id_max_status_length_2",
        "next_shipping_fee_avg_3_prior",
        "prev_shipping_fee_avg_3_prior",
        "prev_event_seq",
    )
)
