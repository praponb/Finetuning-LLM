from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id")
w2 = Window.partitionBy("status").orderBy(F.col("discount").desc(), "id")
w3 = Window.partitionBy("store_id", "channel")
w_recent = Window.partitionBy(F.month("order_date"), "channel").orderBy("amount", "id")
w5 = Window.orderBy("amount", "id")
w_grp = Window.partitionBy("channel").orderBy("row_seq", "id").rowsBetween(-2, Window.currentRow)
w7 = Window.partitionBy("customer_id", "channel").orderBy("prev_discount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w8 = Window.partitionBy("customer_id").orderBy("prev_shipping_fee", "id")
w9 = Window.partitionBy("channel").orderBy("row_seq", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w10 = Window.orderBy("shipping_fee", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w11 = Window.partitionBy("channel").orderBy("next_corr_shipping_fee_amount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_hist = Window.partitionBy("channel").orderBy("amount", "id").rowsBetween(-2, -1)

with_windows = (
    df_orders
    .select(
        "id",
        "customer_id",
        "channel",
        "amount",
        "discount",
        "shipping_fee",
        F.corr("shipping_fee", "discount").over(w).alias("corr_shipping_fee_discount"),
        F.row_number().over(w2).alias("row_seq"),
        F.corr("shipping_fee", "amount").over(w3).alias("corr_shipping_fee_amount"),
        F.lag("discount", 1, 0).over(w_recent).alias("prev_discount"),
        F.lag("shipping_fee", 1).over(w5).alias("prev_shipping_fee"),
    )
)

with_windows_orders = (
    with_windows
    .select(
        "id",
        "channel",
        "amount",
        "shipping_fee",
        "row_seq",
        "prev_discount",
        "prev_shipping_fee",
        F.avg("shipping_fee").over(w_grp).alias("shipping_fee_avg_2"),
        F.first("amount", ignorenulls=True).over(w7).alias("first_known_amount"),
        F.lead("corr_shipping_fee_amount", 2).over(w8).alias("next_corr_shipping_fee_amount"),
        F.sum("row_seq").over(w9).alias("cumulative_row_seq"),
        F.sum("discount").over(w10).alias("cumulative_discount"),
    )
)

valid = (
    with_windows_orders
    .filter(F.col("prev_shipping_fee") > 10)
    .select(
        "id",
        "channel",
        "amount",
        "shipping_fee",
        "prev_discount",
        "prev_shipping_fee",
        "shipping_fee_avg_2",
        "first_known_amount",
        "next_corr_shipping_fee_amount",
        "cumulative_row_seq",
        F.when(F.col("channel") == "store", 1).otherwise(0).alias("is_store"),
        F.coalesce("channel", F.lit("NONE")).alias("resolved_channel"),
    )
)

scored = (
    valid
    .filter(F.col("cumulative_row_seq").between(13, 14))
    .select(
        "id",
        "amount",
        "prev_shipping_fee",
        "next_corr_shipping_fee_amount",
        "cumulative_row_seq",
        "is_store",
        F.sum("next_corr_shipping_fee_amount").over(w11).alias("cumulative_next_corr_shipping_fee_amount"),
        F.avg("prev_shipping_fee").over(w_hist).alias("prev_shipping_fee_avg_2_prior"),
    )
)

filtered = (
    scored
    .filter(F.col("prev_shipping_fee") > 11)
    .select(
        "id",
        "next_corr_shipping_fee_amount",
        "cumulative_row_seq",
        "prev_shipping_fee_avg_2_prior",
        F.least("cumulative_next_corr_shipping_fee_amount", "is_store").alias("least_cumulative_next_corr_shipping_fee_amount_is_store"),
    )
)

result = (
    filtered
    .select(
        "id",
        "next_corr_shipping_fee_amount",
        "prev_shipping_fee_avg_2_prior",
        "least_cumulative_next_corr_shipping_fee_amount_is_store",
    )
    .orderBy(F.col("prev_shipping_fee_avg_2_prior").desc())
)
