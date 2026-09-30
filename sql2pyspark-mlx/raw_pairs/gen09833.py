from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_ordered = Window.orderBy("shipping_fee", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("store_id")
w3 = Window.partitionBy("customer_id").orderBy("amount", "id").rowsBetween(-5, -1)
w4 = Window.partitionBy("customer_id").orderBy(F.col("discount").desc(), "id")
w5 = Window.orderBy("amount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.partitionBy("store_id").orderBy("cumulative_discount", "id")
w7 = Window.orderBy("corr_amount_shipping_fee", "id")
w_grp = Window.partitionBy("status").orderBy("cumulative_discount", "id")

enriched = (
    df_orders
    .filter(F.col("channel").isin(["web", "partner", "store"]))
    .select(
        "id",
        "store_id",
        "status",
        "channel",
        "amount",
        "shipping_fee",
        F.sum("discount").over(w_ordered).alias("cumulative_discount"),
        F.corr("amount", "shipping_fee").over(w2).alias("corr_amount_shipping_fee"),
        F.avg("shipping_fee").over(w3).alias("shipping_fee_avg_5_prior"),
        F.ntile(4).over(w4).alias("discount_bucket"),
        F.sum("shipping_fee").over(w5).alias("cumulative_shipping_fee"),
    )
)

not_flagged = (
    enriched
    .alias("s")
    .join(df_payments.alias("c"), (F.col("c.order_id") == F.col("s.id")) & (F.col("c.paid_at") < "2024-05-13"), "left_anti")
)

not_flagged_orders = (
    not_flagged
    .alias("s")
    .join(df_payments.alias("x"), (F.col("x.order_id") == F.col("s.id")) & (F.col("x.paid_at") >= "2024-08-01"), "left_semi")
)

not_flagged_orders_2 = (
    not_flagged_orders
    .alias("s")
    .join(df_shipments.alias("ch"), (F.col("ch.order_id") == F.col("s.id")) & (F.col("ch.carrier").isin(["UPS", "Flash"])), "left_semi")
)

with_windows = (
    not_flagged_orders_2
    .select(
        "id",
        "status",
        "channel",
        "amount",
        "shipping_fee",
        "cumulative_discount",
        "corr_amount_shipping_fee",
        "discount_bucket",
        "cumulative_shipping_fee",
        F.lag("shipping_fee", 1).over(w6).alias("prev_shipping_fee"),
        F.lead("cumulative_discount", 1).over(w7).alias("next_cumulative_discount"),
        F.row_number().over(w_grp).alias("seq_no"),
    )
)

result = (
    with_windows
    .select(
        "id",
        "status",
        "channel",
        "cumulative_discount",
        "prev_shipping_fee",
        "next_cumulative_discount",
        "seq_no",
        F.when(F.col("channel").rlike("^(partner|store)$"), "CORE").when(F.col("channel").rlike("^(web|mobile)$"), "GROUP_B").otherwise("GROUP_A").alias("channel_zone"),
    )
)
