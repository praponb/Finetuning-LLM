from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy(F.col("amount").desc_nulls_first(), "id")
w2 = Window.partitionBy("status").orderBy("order_date", "id")
w3 = Window.partitionBy("order_id", "status").orderBy(F.col("order_date").desc(), "id")
w4 = Window.orderBy("shipping_fee", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("status").orderBy(F.col("order_date").desc(), "id")
w6 = Window.partitionBy("status").orderBy("shipping_fee", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w7 = Window.partitionBy("status").orderBy("cumulative_discount_2", "id")
w8 = Window.orderBy(F.col("event_seq").desc())

latest = (
    df_payments
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") == 1)
    .select("id", "order_id", "status")
)

enriched = (
    latest
    .alias("e")
    .join(df_orders.alias("d"), F.col("e.order_id") == F.col("d.id"), "left")
    .select(
        "e.id",
        "e.order_id",
        "e.status",
        F.col("d.discount"),
        F.col("d.order_date"),
        F.col("d.shipping_fee"),
    )
)

with_windows = (
    enriched
    .select(
        "id",
        "order_id",
        "status",
        "discount",
        F.lead("discount", 2).over(w2).alias("next_discount"),
        F.row_number().over(w3).alias("event_seq"),
        F.sum("discount").over(w4).alias("cumulative_discount"),
        F.row_number().over(w5).alias("row_seq"),
        F.sum("discount").over(w6).alias("cumulative_discount_2"),
    )
)

scored = (
    with_windows
    .select(
        "id",
        "order_id",
        "status",
        "next_discount",
        "event_seq",
        "cumulative_discount_2",
        F.lead("event_seq").over(w7).alias("next_event_seq"),
        F.lag("event_seq").over(w7).alias("prev_event_seq"),
        F.lead("discount").over(w7).alias("next_discount_2"),
        F.row_number().over(w7).alias("row_seq_2"),
        F.percent_rank().over(w8).alias("event_seq_percent_rank"),
    )
)

with_dims = (
    scored
    .alias("a")
    .join(df_orders.alias("d"), F.col("a.order_id").eqNullSafe(F.col("d.id")), "left")
    .filter(F.col("d.amount") < 1400)
    .select("a.*", F.col("d.discount"), F.col("d.status").alias("order_status"))
)

result = (
    with_dims
    .alias("s")
    .join(df_orders.alias("p"), F.col("s.order_id") == F.col("p.id"), "inner")
    .groupingSets(
        [["channel", "payment_status"], ["channel"], []],
        F.col("p.channel").alias("channel"),
        F.col("s.status").alias("payment_status"),
    )
    .agg(
        F.count("*").alias("rows_total"),
        F.avg(F.col("s.event_seq")).alias("avg_event_seq"),
        F.max(F.col("s.event_seq_percent_rank")).alias("max_event_seq_percent_rank"),
    )
)
