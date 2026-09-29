from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy("paid_at", "id")
w2 = Window.partitionBy("order_id").orderBy("paid_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

filtered = (
    df_payments
    .filter(F.col("order_id").isNotNull() & (F.col("paid_at") < "2024-06-04"))
    .select(
        "id",
        "order_id",
        "method",
        "amount",
        "paid_at",
        "status",
        F.coalesce("method", F.lit("N/A")).alias("method_filled"),
    )
)

valid = (
    filtered
    .filter(F.col("method").isNotNull())
    .withColumn("status_prefix", F.substring("status", 1, 3))
    .withColumn("resolved_method", F.coalesce("method", F.lit("UNKNOWN")))
)

flagged = (
    valid
    .withColumn("is_new_session", F.when(F.lag("paid_at").over(w).isNull() | (F.col("paid_at") > F.lag("paid_at").over(w) + F.expr("INTERVAL 7 DAYS")), 1).otherwise(0))
)

islands = flagged.withColumn("session_id", F.sum("is_new_session").over(w2))

with_dims = (
    islands
    .alias("a")
    .join(df_orders.alias("o"), F.col("a.order_id").eqNullSafe(F.col("o.id")), "left")
    .select(
        "a.*",
        F.col("o.discount"),
        F.col("o.status").alias("order_status"),
        F.col("o.channel"),
    )
)

grouped = (
    with_dims
    .withColumn("amount_tier", F.when(F.col("amount") >= 1500, "LARGE").otherwise("SMALL"))
    .withColumn("activity_month", F.date_trunc("MONTH", "paid_at"))
    .withColumn("discount_tier", F.when(F.col("discount") >= 0.41, "PREMIUM").otherwise("STANDARD"))
    .groupingSets(
        [["amount_tier", "activity_month", "discount_tier"], ["amount_tier", "activity_month"], ["amount_tier"], []],
        "amount_tier",
        "activity_month",
        "discount_tier",
    )
    .agg(
        F.countDistinct("id").alias("distinct_ids"),
        F.count_if(F.col("order_status") == "PENDING").alias("pending_events"),
        F.percentile_approx("session_id", [0.25, 0.5, 0.75]).alias("session_id_quartiles"),
    )
)

result = (
    grouped
    .select(
        "amount_tier",
        "discount_tier",
        "distinct_ids",
        "pending_events",
        "session_id_quartiles",
    )
    .orderBy(F.col("amount_tier").asc_nulls_first())
)
