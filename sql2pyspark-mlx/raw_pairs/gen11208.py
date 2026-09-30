from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy("paid_at", "id")
w2 = Window.partitionBy("order_id").orderBy("paid_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("order_id")
w_running = Window.partitionBy("order_id").orderBy(F.col("paid_at").cast("bigint")).rangeBetween(-604800, Window.currentRow)
w_ordered = Window.orderBy("discount", "id")

cleaned = (
    df_payments
    .filter((F.col("paid_at") >= F.lit("2023-08-19").cast("date")) & (F.col("method").eqNullSafe(F.lit("promptpay"))))
    .select(
        "id",
        "order_id",
        "amount",
        "paid_at",
        "status",
        F.date_format("paid_at", "yyyy-MM").alias("paid_at_label"),
    )
)

with_gaps = (
    cleaned
    .withColumn("is_new_session", F.when(F.col("paid_at") - F.lag("paid_at").over(w) > F.expr("INTERVAL 90 DAYS"), 1).otherwise(0))
)

sessions_payments = (
    with_gaps
    .select(
        "id",
        "order_id",
        "amount",
        "paid_at",
        "status",
        F.sum("is_new_session").over(w2).alias("session_no"),
    )
)

joined = (
    sessions_payments
    .alias("a")
    .join(df_orders.alias("o"), F.col("a.order_id") == F.col("o.id"), "left")
    .select(
        "a.id",
        "a.order_id",
        "a.amount",
        "a.paid_at",
        F.col("o.discount"),
        F.col("o.amount").alias("order_amount"),
        F.col("o.order_date"),
    )
)

enriched = (
    joined
    .alias("src")
    .join(df_orders.alias("p"), F.col("src.order_id") == F.col("p.id"), "inner")
    .select("src.*", F.col("p.amount").alias("order_amount_2"))
)

metrics = (
    enriched
    .withColumn("amount_share", F.round((F.col("amount") / F.sum("amount").over(w3)), 4))
    .withColumn("order_amount_avg_7d", F.avg("order_amount").over(w_running))
    .withColumn("discount_bucket", F.ntile(4).over(w_ordered))
)

result = (
    metrics
    .select(
        "id",
        "order_id",
        F.col("order_amount_2").alias("ORDER_AMOUNT_2"),
        F.col("amount_share").alias("AMOUNT_SHARE"),
        F.col("amount").alias("AMOUNT_VAL"),
    )
    .unpivot(
        ids=["id", "order_id"],
        values=["ORDER_AMOUNT_2", "AMOUNT_SHARE", "AMOUNT_VAL"],
        variableColumnName="kpi",
        valueColumnName="metric_value",
    )
)
