from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id").orderBy("least_shipping_fee_discount", "id")
w2 = Window.partitionBy(F.date_trunc("YEAR", "order_date"), "status").orderBy("shipping_fee", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("customer_id").orderBy("next_shipping_fee_per_prev_amount", "id").rowsBetween(-5, Window.currentRow)
w4 = Window.partitionBy("customer_id").orderBy("row_seq", "id")

filtered = (
    df_orders
    .filter(F.col("order_date") > F.lit("2023-09-20").cast("date"))
    .select(
        "id",
        "customer_id",
        "store_id",
        "order_date",
        "status",
        "channel",
        "amount",
        "shipping_fee",
        F.least("shipping_fee", "discount").alias("least_shipping_fee_discount"),
    )
)

metrics = (
    filtered
    .select(
        "id",
        "customer_id",
        F.lead("shipping_fee").over(w).alias("next_shipping_fee"),
        F.lag("amount").over(w).alias("prev_amount"),
        F.row_number().over(w).alias("row_seq"),
        F.first("channel", ignorenulls=True).over(w2).alias("first_known_channel"),
    )
)

filtered_orders = (
    metrics
    .filter(F.col("row_seq") >= 5)
    .select(
        "id",
        "customer_id",
        "next_shipping_fee",
        "prev_amount",
        "row_seq",
        F.round((F.col("next_shipping_fee") / F.col("prev_amount")), 2).alias("next_shipping_fee_per_prev_amount"),
    )
)

enriched = (
    filtered_orders
    .filter(F.col("next_shipping_fee").between(13, 36))
    .select(
        "id",
        "next_shipping_fee",
        F.avg("prev_amount").over(w3).alias("prev_amount_avg_5"),
        F.lag("next_shipping_fee_per_prev_amount", 1).over(w4).alias("prev_next_shipping_fee_per_prev_amount"),
    )
)

kept_keys = (
    enriched
    .select("id")
    .intersectAll(
        df_orders
        .filter(F.col("amount") < 1060)
        .select("id")
    )
)

verified = (
    enriched
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

kept_keys_2 = (
    verified
    .select("id")
    .intersectAll(
        df_orders
        .filter(~F.col("channel").isin(["web", "store"]))
        .select("id")
    )
)

kept_rows = (
    verified
    .alias("s")
    .join(kept_keys_2.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

not_flagged = (
    kept_rows
    .alias("s")
    .join(df_payments.alias("ch"), (F.col("ch.order_id") == F.col("s.id")) & (F.col("ch.order_id").isNull()), "left_semi")
)

result = (
    not_flagged
    .select(
        "id",
        "next_shipping_fee",
        "prev_amount_avg_5",
        "prev_next_shipping_fee_per_prev_amount",
    )
    .orderBy(F.col("next_shipping_fee").desc(), "id")
)
