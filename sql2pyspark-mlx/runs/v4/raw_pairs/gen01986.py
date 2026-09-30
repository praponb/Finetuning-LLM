from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("method").orderBy("paid_at", "id")
w2 = Window.partitionBy("method").orderBy("paid_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

with_dims = (
    df_payments
    .alias("src")
    .join(df_orders.alias("b"), F.col("src.order_id") == F.col("b.id"), "inner")
    .select("src.*", F.col("b.shipping_fee"), F.col("b.discount"))
)

with_dims_payments = (
    with_dims
    .alias("src")
    .join(df_orders.alias("b"), F.col("src.order_id") == F.col("b.id"), "left")
    .select(
        "src.*",
        F.col("b.amount").alias("order_amount"),
        F.col("b.status").alias("order_status"),
        F.col("b.order_date"),
    )
)

prev_events = with_dims_payments.withColumn("prev_paid_at", F.lag("paid_at").over(w))

gaps = (
    prev_events
    .withColumn("new_group_flag", F.when(F.col("prev_paid_at").isNull() | (F.col("paid_at") > F.col("prev_paid_at") + F.expr("INTERVAL 30 DAYS")), 1).otherwise(0))
)

islands = gaps.withColumn("session_id", F.sum("new_group_flag").over(w2))

result = (
    islands
    .select(
        "id",
        "method",
        "amount",
        "paid_at",
        "status",
        "shipping_fee",
        "discount",
        "order_status",
        "order_date",
        F.when(F.col("method").rlike("^(bank_transfer|promptpay)$"), "CORE").when(F.col("method").rlike("^(card|paypal)$"), "GROUP_A").otherwise("PRIORITY").alias("method_zone"),
    )
    .orderBy(F.col("amount").desc_nulls_last())
)
