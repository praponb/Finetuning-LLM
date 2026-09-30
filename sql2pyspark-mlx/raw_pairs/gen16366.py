from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("channel").orderBy("order_amount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_grp = Window.partitionBy("method").orderBy(F.col("paid_at").cast("bigint")).rangeBetween(-2592000, Window.currentRow)
w3 = Window.orderBy("order_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_ordered = Window.partitionBy(F.month("order_date"), "status").orderBy("order_amount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("status", "order_id").orderBy("paid_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.partitionBy("method").orderBy("paid_at", "id")
w7 = Window.partitionBy("method").orderBy("paid_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w8 = Window.partitionBy(F.year("paid_at"), "status").orderBy("order_amount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w9 = Window.partitionBy(F.to_date("paid_at"), "status").orderBy("order_amount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w10 = Window.partitionBy("status").orderBy(F.col("paid_at").cast("bigint")).rangeBetween(-2592000, Window.currentRow)

joined = (
    df_payments
    .alias("s")
    .join(df_orders.alias("o"), F.col("s.order_id").eqNullSafe(F.col("o.id")), "left")
    .filter(F.col("o.status").isin(["COMPLETED", "SHIPPED", "RETURNED"]))
    .select(
        "s.id",
        "s.order_id",
        "s.method",
        "s.paid_at",
        "s.status",
        F.col("o.amount").alias("order_amount"),
        F.col("o.channel"),
        F.col("o.order_date"),
    )
)

metrics = (
    joined
    .withColumn("first_known_method", F.first("method", ignorenulls=True).over(w))
    .withColumn("order_amount_count_30d", F.count("order_amount").over(w_grp))
    .withColumn("last_known_method", F.last("method", ignorenulls=True).over(w3))
    .withColumn("last_known_method_2", F.last("method", ignorenulls=True).over(w_ordered))
    .withColumn("running_order_amount", F.sum("order_amount").over(w5))
)

with_prev = metrics.withColumn("prev_paid_at", F.lag("paid_at").over(w6))

with_gaps = (
    with_prev
    .withColumn("is_new_burst", F.when(F.col("prev_paid_at").isNull() | (F.col("paid_at") > F.col("prev_paid_at") + F.expr("INTERVAL 30 DAYS")), 1).otherwise(0))
)

islands = (
    with_gaps
    .select(
        "id",
        "method",
        "paid_at",
        "status",
        "order_amount",
        "first_known_method",
        "last_known_method_2",
        F.sum("is_new_burst").over(w7).alias("session_id"),
    )
)

enriched = (
    islands
    .select(
        "*",
        F.last("method", ignorenulls=True).over(w8).alias("last_known_method"),
        F.sum("session_id").over(w9).alias("cumulative_session_id"),
        F.sum("session_id").over(w10).alias("session_id_sum_30d"),
    )
)

result = (
    enriched
    .withColumn("order_amount_tier", F.when(F.col("order_amount") >= 1760, "LARGE").otherwise("SMALL"))
    .withColumn("session_id_tier", F.when(F.col("session_id") >= 8, "LARGE").otherwise("SMALL"))
    .cube("order_amount_tier", "session_id_tier")
    .agg(
        F.percentile_approx("session_id_sum_30d", [0.25, 0.5, 0.75]).alias("session_id_sum_30d_quartiles"),
        F.count_if(F.col("status") == "REFUNDED").alias("refunded_events"),
        F.percentile_approx("session_id", 0.5).alias("session_id_p50"),
        F.grouping("order_amount_tier").alias("grouping_level"),
    )
)
