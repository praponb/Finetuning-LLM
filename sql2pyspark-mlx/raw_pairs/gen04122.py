from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("channel").orderBy("shipping_fee", "id")
w2 = Window.partitionBy("channel").orderBy("order_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_grp = Window.partitionBy("status", "channel").orderBy("last_known_shipping_fee", "id")
w4 = Window.partitionBy("store_id").orderBy(F.col("next_prev_discount").desc(), "id")
w5 = Window.partitionBy("status", "store_id").orderBy(F.col("next_prev_discount").desc())
w6 = Window.partitionBy(F.year("order_date"), "status").orderBy("row_seq", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

scored = (
    df_orders
    .withColumn("prev_shipping_fee", F.lag("shipping_fee").over(w))
    .withColumn("next_discount", F.lead("discount").over(w))
    .withColumn("prev_discount", F.lag("discount").over(w))
    .withColumn("event_seq", F.row_number().over(w))
    .withColumn("last_known_shipping_fee", F.last("shipping_fee", ignorenulls=True).over(w2))
)

valid = (
    scored
    .filter(F.col("store_id").isNotNull() & (F.col("store_id") == 39))
    .select(
        "*",
        F.round((F.col("event_seq") * 0.85), 2).alias("event_seq_adjusted"),
        F.coalesce("channel", F.lit("N/A")).alias("resolved_channel"),
    )
)

with_windows = (
    valid
    .select(
        "*",
        F.lag("prev_discount").over(w_grp).alias("prev_prev_discount"),
        F.lead("prev_discount").over(w_grp).alias("next_prev_discount"),
        F.lead("amount").over(w_grp).alias("next_amount"),
        F.row_number().over(w_grp).alias("row_seq"),
    )
)

metrics = (
    with_windows
    .filter(F.col("discount") < 0.26)
    .select(
        "*",
        F.row_number().over(w4).alias("seq_no"),
        F.rank().over(w5).alias("next_prev_discount_rank"),
        F.sum("row_seq").over(w6).alias("running_row_seq"),
    )
)

result = (
    metrics
    .select(
        "id",
        "order_date",
        "status",
        "channel",
        "discount",
        "prev_shipping_fee",
        "next_discount",
        "prev_discount",
        "event_seq",
        F.when(F.col("status").rlike("^(PENDING|CANCELLED)$"), "GROUP_A").when(F.col("status").rlike("^(COMPLETED|SHIPPED)$"), "GROUP_B").otherwise("OTHER_ZONE").alias("status_zone"),
    )
    .orderBy(F.col("id").desc_nulls_first(), "status_zone")
)
