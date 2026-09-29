from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy("paid_at", "id")
w2 = Window.partitionBy("order_id").orderBy("paid_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("order_id").orderBy(F.col("amount").asc_nulls_first(), "id")
cost_by_order_id = df_shipments.groupBy("order_id").agg(F.avg("cost").alias("avg_shipment_cost"))

filtered = (
    df_payments
    .filter(F.col("amount") >= 920)
    .select(
        "id",
        "order_id",
        "amount",
        "paid_at",
        F.substring("status", 1, 4).alias("status_prefix"),
        F.coalesce("method", F.lit("UNKNOWN")).alias("method_filled"),
    )
)

prev_events = filtered.withColumn("prev_paid_at", F.lag("paid_at").over(w))

flagged = (
    prev_events
    .withColumn("is_new_session", F.when(F.col("prev_paid_at").isNull() | (F.col("paid_at") > F.col("prev_paid_at") + F.expr("INTERVAL 30 DAYS")), 1).otherwise(0))
)

sessionized = flagged.withColumn("burst_id", F.sum("is_new_session").over(w2))

with_dims = (
    sessionized
    .alias("s")
    .join(df_orders.alias("b"), F.col("s.order_id") == F.col("b.id"), "left")
    .select("s.*", F.col("b.discount"), F.col("b.amount").alias("order_amount"))
)

with_gaps = (
    with_dims
    .withColumn("new_group_flag", F.when(F.col("paid_at") - F.lag("paid_at").over(w) > F.expr("INTERVAL 30 DAYS"), 1).otherwise(0))
)

sessionized_payments = with_gaps.withColumn("burst_id_2", F.sum("new_group_flag").over(w2))

deduped = (
    sessionized_payments
    .withColumn("latest_rank", F.row_number().over(w3))
    .filter(F.col("latest_rank") == 1)
)

result = (
    deduped
    .join(cost_by_order_id, on="order_id", how="left")
    .select(
        "id",
        "order_id",
        "status_prefix",
        "method_filled",
        "prev_paid_at",
        "burst_id",
        "discount",
        "order_amount",
        "new_group_flag",
        F.col("avg_shipment_cost"),
    )
    .orderBy(F.col("avg_shipment_cost").desc())
)
