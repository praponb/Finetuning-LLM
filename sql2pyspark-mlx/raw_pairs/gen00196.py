from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_grp = Window.orderBy(F.col("paid_at").cast("bigint")).rangeBetween(-604800, Window.currentRow)
w2 = Window.partitionBy("order_id", "method").orderBy("amount", "id")
w3 = Window.orderBy("order_amount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_recent = Window.partitionBy("method").orderBy("paid_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("order_id").orderBy("paid_at", "id")
w6 = Window.partitionBy("order_id").orderBy("paid_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_part = Window.partitionBy("order_id").orderBy("cumulative_order_amount", "id")
w8 = Window.partitionBy("method").orderBy("event_seq")

base = (
    df_payments
    .filter((F.col("status") == "SUCCESS") & F.col("order_id").isNotNull())
    .select(
        "*",
        F.date_trunc("WEEK", "paid_at").alias("paid_at_period"),
        F.when(F.col("method").isin(["paypal", "bank_transfer"]), "Group A").otherwise("Other").alias("method_group"),
    )
)

with_dims = (
    base
    .alias("a")
    .join(df_orders.alias("b"), F.col("a.order_id") == F.col("b.id"), "left")
    .select(
        "a.id",
        "a.order_id",
        "a.method",
        "a.amount",
        "a.paid_at",
        "a.method_group",
        F.col("b.amount").alias("order_amount"),
    )
)

metrics = (
    with_dims
    .filter(F.col("order_amount").between(510, 1020))
    .select(
        "*",
        F.count("order_amount").over(w_grp).alias("order_amount_count_7d"),
        F.ntile(10).over(w2).alias("amount_bucket"),
        F.first("amount", ignorenulls=True).over(w3).alias("first_known_amount"),
        F.sum("order_amount").over(w_recent).alias("cumulative_order_amount"),
    )
)

gaps = (
    metrics
    .withColumn("is_new_burst", F.when(F.lag("paid_at").over(w5).isNull() | (F.col("paid_at") > F.lag("paid_at").over(w5) + F.expr("INTERVAL 14 DAYS")), 1).otherwise(0))
)

sessionized = gaps.withColumn("island_id", F.sum("is_new_burst").over(w6))

metrics_payments = (
    sessionized
    .select(
        "*",
        F.lag("is_new_burst").over(w_part).alias("prev_is_new_burst"),
        F.lead("is_new_burst").over(w_part).alias("next_is_new_burst"),
        F.lag("amount_bucket").over(w_part).alias("prev_amount_bucket"),
        F.lead("amount_bucket").over(w_part).alias("next_amount_bucket"),
        F.row_number().over(w_part).alias("event_seq"),
    )
)

latest = (
    metrics_payments
    .withColumn("rn", F.rank().over(w8))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

result = (
    latest
    .select(
        "id",
        "method",
        "amount",
        "paid_at",
        "method_group",
        "order_amount_count_7d",
        "first_known_amount",
        "cumulative_order_amount",
        "island_id",
    )
)
