from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.amount").desc(), F.col("c.id").desc())
w2 = Window.partitionBy("channel", "status")
w3 = Window.partitionBy("status", "channel").orderBy(F.col("paid_at").cast("bigint")).rangeBetween(-7776000, Window.currentRow)
w4 = Window.partitionBy("channel").orderBy("discount", "id")
w5 = Window.partitionBy("channel").orderBy("paid_at", "id")
w6 = Window.partitionBy("channel").orderBy("paid_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w7 = Window.partitionBy("status").orderBy(F.col("session_no").desc(), "id")
w8 = Window.partitionBy("channel").orderBy("channel_sum_amount")
w9 = Window.partitionBy("status").orderBy(F.col("is_new_session").desc(), "id")
w10 = Window.partitionBy("channel", "status").orderBy(F.col("rank_in_group").desc(), "id")
w11 = Window.orderBy(F.col("discount").asc_nulls_first())

base = (
    df_orders
    .filter(F.col("order_date").between("2023-04-01", "2023-07-15"))
    .withColumn("years_since_order_date", (F.datediff(F.current_date(), "order_date") / 365.25))
)

with_latest = (
    base
    .alias("s")
    .join(df_payments.alias("c"), (F.col("c.order_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.status",
        "s.channel",
        "s.amount",
        "s.discount",
        F.col("c.paid_at"),
    )
)

scored = (
    with_latest
    .select(
        "*",
        F.sum("amount").over(w2).alias("channel_sum_amount"),
        F.count("discount").over(w3).alias("discount_count_90d"),
        F.lag("discount", 2).over(w4).alias("prev_discount"),
    )
)

gaps = (
    scored
    .withColumn("is_new_session", F.when(F.lag("paid_at").over(w5).isNull() | (F.col("paid_at") > F.lag("paid_at").over(w5) + F.expr("INTERVAL 30 DAYS")), 1).otherwise(0))
)

islands = gaps.withColumn("session_no", F.sum("is_new_session").over(w6))

latest = (
    islands
    .withColumn("rank_in_group", F.row_number().over(w7))
    .filter(F.col("rank_in_group") == 1)
)

not_flagged = (
    latest
    .alias("s")
    .join(df_order_items.alias("c"), (F.col("c.order_id") == F.col("s.id")) & (F.col("c.order_id").isin([1, 18, 23])), "left_semi")
)

metrics = (
    not_flagged
    .filter(F.col("status") != "COMPLETED")
    .select(
        "id",
        "status",
        "channel",
        "discount",
        "paid_at",
        "channel_sum_amount",
        "is_new_session",
        "session_no",
        "rank_in_group",
        F.cume_dist().over(w8).alias("channel_sum_amount_cume_dist"),
        F.row_number().over(w9).alias("seq_no"),
        F.row_number().over(w10).alias("row_seq"),
        F.percent_rank().over(w11).alias("discount_percent_rank"),
    )
)

result = (
    metrics
    .groupBy(
        F.when(F.col("channel_sum_amount") >= 7994, "LARGE").otherwise("SMALL").alias("channel_sum_amount_tier"),
    )
    .agg(
        F.max("seq_no").alias("max_seq_no"),
        F.count("*").alias("row_count"),
        F.sum(F.when(F.col("status") == "PENDING", F.col("channel_sum_amount_cume_dist")).otherwise(0)).alias("pending_channel_sum_amount_cume_dist"),
        F.sum("discount_percent_rank").alias("total_discount_percent_rank"),
        F.avg(F.when(F.col("channel") == "store", F.col("session_no"))).alias("store_avg_session_no"),
    )
    .filter(F.col("row_count") > 1)
    .orderBy("total_discount_percent_rank")
)
