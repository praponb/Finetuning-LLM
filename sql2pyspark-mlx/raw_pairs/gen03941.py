from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.unit_price").desc(), F.col("c.id").desc())
w2 = Window.partitionBy("customer_id").orderBy(F.col("order_date").desc(), "id")
w3 = Window.partitionBy("customer_id", "channel").orderBy(F.col("unit_price").desc_nulls_last())

with_last = (
    df_orders
    .alias("s")
    .join(df_order_items.alias("c"), (F.col("c.order_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.customer_id",
        "s.store_id",
        "s.order_date",
        "s.channel",
        F.col("c.unit_price"),
    )
)

with_windows = (
    with_last
    .filter(F.col("unit_price") < 170)
    .withColumn("row_seq", F.row_number().over(w2))
    .withColumn("unit_price_dense_rank", F.dense_rank().over(w3))
)

not_flagged = (
    with_windows
    .alias("s")
    .join(df_payments.alias("ch"), (F.col("ch.order_id") == F.col("s.id")) & (F.col("ch.paid_at") < F.col("s.order_date")), "left_anti")
)

with_window = (
    not_flagged
    .alias("e")
    .join(df_subscriptions.alias("sub"), (F.col("e.customer_id") == F.col("sub.customer_id")) & F.col("e.order_date").between(F.col("sub.start_date"), F.col("sub.end_date")), "left")
    .select(
        "e.*",
        F.col("sub.id").alias("subscription_id"),
        F.col("sub.plan"),
        F.col("sub.monthly_fee"),
    )
)

result = (
    with_window
    .groupBy("customer_id", "plan")
    .pivot("channel", ["mobile", "partner"])
    .agg(F.sum("unit_price_dense_rank").alias("amount"), F.count(F.lit(1)).alias("n"))
)
