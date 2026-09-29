from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("method").orderBy("paid_at", "id")
w2 = Window.partitionBy("method").orderBy("paid_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("channel").orderBy("order_order_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy(F.month("paid_at_date"), "channel").orderBy("amount", "id").rowsBetween(-5, Window.currentRow)

gaps = (
    df_payments
    .withColumn("session_start_flag", F.when(F.col("paid_at") - F.lag("paid_at").over(w) > F.expr("INTERVAL 14 DAYS"), 1).otherwise(0))
)

sessionized = gaps.withColumn("burst_id", F.sum("session_start_flag").over(w2))

base = (
    sessionized
    .filter(F.col("order_id").isNotNull() & ~F.col("method").isin(["card", "bank_transfer"]))
    .withColumn("paid_at_date", F.to_date("paid_at"))
    .withColumn("burst_id_num", F.col("burst_id").cast("string"))
    .withColumn("method_filled", F.coalesce("method", F.lit("N/A")))
)

with_dims = (
    base
    .alias("a")
    .join(df_orders.alias("p"), F.col("a.order_id") == F.col("p.id"), "left")
    .select(
        "a.*",
        F.col("p.discount"),
        F.col("p.amount").alias("order_amount"),
        F.col("p.order_date"),
    )
)

joined = (
    with_dims
    .alias("a")
    .join(df_orders.alias("d"), F.col("a.order_id") == F.col("d.id"), "left")
    .select("a.*", F.col("d.order_date").alias("order_order_date"), F.col("d.channel"))
)

metrics = (
    joined
    .filter(F.col("status") == "SUCCESS")
    .select(
        "id",
        "order_id",
        "method",
        "amount",
        "paid_at",
        "status",
        "burst_id",
        "paid_at_date",
        "burst_id_num",
        "method_filled",
        "discount",
        "order_amount",
        "order_order_date",
        "channel",
        F.min("amount").over(w3).alias("running_min_amount"),
        F.max("amount").over(w3).alias("running_max_amount"),
        F.avg("amount").over(w3).alias("running_avg_amount"),
        F.avg("discount").over(w4).alias("discount_avg_5"),
    )
)

result = (
    metrics
    .alias("src")
    .join(df_orders.alias("d"), F.col("src.order_id") == F.col("d.id"), "inner")
    .groupingSets(
        [["order_channel", "order_status", "payment_channel"], ["order_channel", "order_status"], ["order_channel"], []],
        F.col("d.channel").alias("order_channel"),
        F.col("d.status").alias("order_status"),
        F.col("src.channel").alias("payment_channel"),
    )
    .agg(
        F.count("*").alias("rows_total"),
        F.min(F.col("src.running_avg_amount")).alias("min_running_avg_amount"),
        F.max(F.col("src.running_min_amount")).alias("max_running_min_amount"),
        F.grouping_id().alias("level_id"),
    )
)
