from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.date_trunc("YEAR", "order_date"), "customer_id").orderBy("order_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.orderBy("discount", "id")
w3 = Window.orderBy("amount", "id")
w_grp = Window.partitionBy("status", "store_id").orderBy("seq_no", "id")
w5 = Window.partitionBy("store_id").orderBy("next_event_seq", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.partitionBy("channel").orderBy("seq_no_2", "id")
w_recent = Window.partitionBy(F.year("order_date"), "channel").orderBy(F.col("next_seq_no").asc_nulls_last())
w8 = Window.partitionBy(F.month("order_date"), "channel")
w9 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.amount").desc(), F.col("c.id").desc())

scored = (
    df_orders
    .withColumn("running_discount", F.sum("discount").over(w))
    .withColumn("seq_no", F.row_number().over(w2))
    .withColumn("event_seq", F.row_number().over(w3))
)

eligible = (
    scored
    .alias("s")
    .join(df_order_items.alias("x"), (F.col("x.order_id") == F.col("s.id")) & (F.col("x.order_id").isin([9, 12, 24])), "left_anti")
)

enriched = (
    eligible
    .filter(F.col("seq_no").isNull())
    .withColumn("next_event_seq", F.lead("event_seq").over(w_grp))
    .withColumn("next_seq_no", F.lead("seq_no").over(w_grp))
    .withColumn("seq_no_2", F.row_number().over(w_grp))
)

scored_orders = (
    enriched
    .withColumn("running_max_running_discount", F.max("running_discount").over(w5))
    .withColumn("running_count_running_discount", F.count("running_discount").over(w5))
    .withColumn("running_sum_running_discount", F.sum("running_discount").over(w5))
    .withColumn("next_discount", F.lead("discount", 1).over(w6))
)

scored_orders_2 = (
    scored_orders
    .select(
        "id",
        "discount",
        "shipping_fee",
        "seq_no",
        "next_seq_no",
        "running_max_running_discount",
        "next_discount",
        F.cume_dist().over(w_recent).alias("next_seq_no_cume_dist"),
        F.max("running_max_running_discount").over(w8).alias("order_date_month_max_running_max_running_discount"),
    )
)

with_latest = (
    scored_orders_2
    .alias("s")
    .join(df_payments.alias("c"), (F.col("c.order_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w9))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.method"), F.col("c.paid_at"))
)

matched_keys = (
    with_latest
    .select("id")
    .exceptAll(
        df_orders
        .filter(F.col("channel").isin(["mobile", "partner"]))
        .select("id")
    )
)

reconciled = (
    with_latest
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

result = (
    reconciled
    .select(
        "id",
        "shipping_fee",
        "seq_no",
        "next_seq_no",
        "running_max_running_discount",
        "next_discount",
        "next_seq_no_cume_dist",
        "paid_at",
    )
)
