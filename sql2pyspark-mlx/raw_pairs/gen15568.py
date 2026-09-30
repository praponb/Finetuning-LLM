from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("channel", "store_id").orderBy("order_date", "id")
w2 = Window.partitionBy("channel").orderBy("amount", "id")
w_part = Window.partitionBy(F.month("order_date"), "channel")
w_running = Window.orderBy("discount_int", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_grp = Window.partitionBy("store_id").orderBy("row_seq", "id")
w6 = Window.partitionBy("channel").orderBy("prev_amount", "id")
w7 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.shipped_date").desc(), F.col("c.id").desc())

valid = (
    df_orders
    .filter((F.col("order_date") >= F.to_date(F.lit("2023-07-14"))) & (F.col("order_date") >= "2023-07-19"))
    .select(
        "id",
        "store_id",
        "order_date",
        "channel",
        "amount",
        "shipping_fee",
        F.trim("status").alias("clean_status"),
        F.col("discount").cast("int").alias("discount_int"),
        F.coalesce("channel", F.lit("OTHER")).alias("channel_clean"),
    )
)

metrics = (
    valid
    .filter(F.col("discount_int") > 55)
    .withColumn("next_discount_int", F.lead("discount_int").over(w))
    .withColumn("prev_amount", F.lag("amount").over(w))
    .withColumn("row_seq", F.row_number().over(w))
)

metrics_orders = (
    metrics
    .withColumn("next_prev_amount", F.lead("prev_amount", 1).over(w2))
    .withColumn("order_date_month_sum_shipping_fee", F.sum("shipping_fee").over(w_part))
    .withColumn("cumulative_next_discount_int", F.sum("next_discount_int").over(w_running))
)

metrics_orders_2 = (
    metrics_orders
    .select(
        "id",
        "store_id",
        "order_date",
        "channel",
        "amount",
        "channel_clean",
        "prev_amount",
        "row_seq",
        F.lead("next_discount_int").over(w_grp).alias("next_next_discount_int"),
        F.lag("order_date_month_sum_shipping_fee").over(w_grp).alias("prev_order_date_month_sum_shipping_fee"),
        F.row_number().over(w_grp).alias("row_seq_2"),
        F.ntile(4).over(w6).alias("prev_amount_bucket"),
    )
)

with_last = (
    metrics_orders_2
    .alias("s")
    .join(df_shipments.alias("c"), (F.col("c.order_id") == F.col("s.id")) & (F.col("c.shipped_date") <= F.col("s.order_date")), "inner")
    .withColumn("rn", F.row_number().over(w7))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.status").alias("shipment_status"), F.col("c.cost"))
)

result = (
    with_last
    .select(
        "id",
        "store_id",
        "order_date",
        "channel",
        "channel_clean",
        "prev_amount",
        "row_seq",
        "next_next_discount_int",
        "prev_order_date_month_sum_shipping_fee",
        F.when(F.col("channel").rlike("^(store|web)$"), "GROUP_A").when(F.col("channel").rlike("^(mobile|partner)$"), "CORE").otherwise("GROUP_B").alias("channel_zone"),
    )
    .orderBy(F.col("row_seq").desc())
)
