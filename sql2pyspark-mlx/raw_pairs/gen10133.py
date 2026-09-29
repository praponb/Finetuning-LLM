from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id", "store_id").orderBy("opened_date", "id")
w2 = Window.partitionBy("store_id").orderBy("sqft_times_prev_discount", "id")
w3 = Window.partitionBy("channel").orderBy("opened_date", "id")

enriched = (
    df_orders
    .alias("s")
    .join(df_stores.alias("d"), F.col("s.store_id") == F.col("d.id"), "left")
    .filter(F.col("d.store_name").isNotNull())
    .select("s.*", F.col("d.sqft"), F.col("d.opened_date"))
)

scored = (
    enriched
    .withColumn("prev_shipping_fee", F.lag("shipping_fee").over(w))
    .withColumn("next_discount", F.lead("discount").over(w))
    .withColumn("prev_discount", F.lag("discount").over(w))
    .withColumn("row_seq", F.row_number().over(w))
)

valid = (
    scored
    .filter(F.col("shipping_fee") >= 17)
    .select(
        "*",
        (F.col("discount") - F.col("row_seq")).alias("discount_minus_row_seq"),
        F.coalesce("channel", F.lit("N/A")).alias("resolved_channel"),
    )
)

filtered = (
    valid
    .filter(F.col("channel").isNotNull())
    .select(
        "id",
        "store_id",
        "order_date",
        "status",
        "channel",
        "discount",
        "sqft",
        "opened_date",
        "prev_shipping_fee",
        "next_discount",
        "prev_discount",
        "row_seq",
        "discount_minus_row_seq",
        (F.col("sqft") * F.col("prev_discount")).alias("sqft_times_prev_discount"),
        F.coalesce("channel", F.lit("UNKNOWN")).alias("channel_clean"),
    )
)

not_flagged = (
    filtered
    .alias("s")
    .join(df_shipments.alias("c"), (F.col("c.order_id") == F.col("s.id")) & (F.col("c.delivered_date") >= F.col("s.order_date")), "left_anti")
)

result = (
    not_flagged
    .select(
        "*",
        F.lag("discount_minus_row_seq", 2, 0).over(w2).alias("prev_discount_minus_row_seq"),
        F.lag("next_discount", 1, 0).over(w3).alias("prev_next_discount"),
    )
)
