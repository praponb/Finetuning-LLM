from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("channel").orderBy("amount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.orderBy("order_date_minus_54d", "id")
w3 = Window.partitionBy("channel")
w4 = Window.partitionBy("channel", "store_id").orderBy("amount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("channel").orderBy("order_date_minus_54d", "id")

kept = (
    df_orders
    .alias("s")
    .join(df_shipments.alias("x"), (F.col("x.order_id") == F.col("s.id")) & (F.col("x.shipped_date") <= F.col("s.order_date")), "left_semi")
)

filtered = (
    kept
    .filter((F.col("order_date") > F.to_date(F.lit("2023-10-31"))) & F.col("customer_id").isNotNull() & F.col("customer_id").isin([2, 13, 17]))
    .select(
        "id",
        "store_id",
        "channel",
        "amount",
        "discount",
        "shipping_fee",
        F.date_sub("order_date", 54).alias("order_date_minus_54d"),
        F.coalesce("channel", F.lit("N/A")).alias("resolved_channel"),
    )
)

not_flagged = (
    filtered
    .alias("s")
    .join(df_payments.alias("ch"), (F.col("ch.order_id") == F.col("s.id")) & (F.col("ch.status").isin(["REFUNDED", "FAILED"])), "left_semi")
)

result = (
    not_flagged
    .select(
        "*",
        F.last("amount", ignorenulls=True).over(w).alias("last_known_amount"),
        F.lead("shipping_fee", 2).over(w2).alias("next_shipping_fee"),
        F.max("shipping_fee").over(w3).alias("channel_max_shipping_fee"),
        F.last("shipping_fee", ignorenulls=True).over(w4).alias("last_known_shipping_fee"),
        F.lead("shipping_fee", 2).over(w5).alias("next_shipping_fee_2"),
    )
)
