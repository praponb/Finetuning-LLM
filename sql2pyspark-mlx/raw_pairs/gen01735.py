from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("method").orderBy("paid_at", "id")
w2 = Window.partitionBy("method").orderBy("paid_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("status").orderBy("paid_at", "id")
w4 = Window.partitionBy("method").orderBy(F.col("paid_at").cast("bigint")).rangeBetween(-2592000, Window.currentRow)
w5 = Window.partitionBy("status").orderBy("session_no")
w6 = Window.partitionBy("method").orderBy(F.col("seq_no").desc_nulls_last(), "id")
w7 = Window.partitionBy("method", "status")
w8 = Window.orderBy("session_no_sum_30d", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

cleaned = (
    df_payments
    .filter(F.col("method").isin(["promptpay", "cash", "card"]) & F.col("order_id").isNotNull())
    .select(
        "id",
        "order_id",
        "method",
        "amount",
        "paid_at",
        "status",
        F.date_format("paid_at", "yyyy-MM-dd").alias("paid_at_label"),
        F.coalesce("method", F.lit("unassigned")).alias("method_filled"),
    )
)

flagged = (
    cleaned
    .withColumn("is_new_session", F.when(F.col("paid_at") - F.lag("paid_at").over(w) > F.expr("INTERVAL 60 DAYS"), 1).otherwise(0))
)

sessions_payments = (
    flagged
    .select(
        "id",
        "method",
        "paid_at",
        "status",
        "paid_at_label",
        F.sum("is_new_session").over(w2).alias("session_no"),
    )
)

metrics = (
    sessions_payments
    .select(
        "id",
        "method",
        "paid_at",
        "status",
        "paid_at_label",
        "session_no",
        F.row_number().over(w3).alias("seq_no"),
        F.sum("session_no").over(w4).alias("session_no_sum_30d"),
        F.rank().over(w5).alias("session_no_rank"),
    )
)

ranked = (
    metrics
    .withColumn("rank_in_group", F.row_number().over(w6))
    .filter(F.col("rank_in_group") == 1)
    .select("id", "method", "status", "paid_at_label", "seq_no", "session_no_sum_30d")
)

metrics_payments = (
    ranked
    .filter(F.col("seq_no") > 5)
    .select(
        "id",
        "method",
        "seq_no",
        "session_no_sum_30d",
        F.avg("session_no_sum_30d").over(w7).alias("method_avg_session_no_sum_30d"),
        F.last("method", ignorenulls=True).over(w8).alias("last_known_method"),
    )
)

kept_keys = (
    metrics_payments
    .select("method", "id")
    .intersect(
        df_payments
        .filter(F.col("amount").isNotNull())
        .select("method", "id")
    )
)

kept_rows = (
    metrics_payments
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.method") == F.col("k.method")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

base = (
    kept_rows
    .filter(F.col("method").isNotNull() & (F.col("method").eqNullSafe(F.lit("cash"))))
    .select(
        "id",
        "method",
        "session_no_sum_30d",
        "method_avg_session_no_sum_30d",
        "last_known_method",
        F.upper("method").alias("method_upper"),
    )
)

stats = (
    base
    .withColumn("session_no_sum_30d_tier", F.when(F.col("session_no_sum_30d") >= 31, "HIGH").otherwise("LOW"))
    .groupingSets(
        [["session_no_sum_30d_tier", "method"], ["session_no_sum_30d_tier"]],
        "session_no_sum_30d_tier",
        "method",
    )
    .agg(
        F.avg(F.when(F.col("method") == "cash", F.col("method_avg_session_no_sum_30d"))).alias("cash_avg_method_avg_session_no_sum_30d"),
        F.sum(F.when(F.col("method") == "paypal", F.col("session_no_sum_30d")).otherwise(0)).alias("paypal_session_no_sum_30d"),
        F.count_if(F.col("method") == "cash").alias("cash_events"),
        F.size(F.collect_set("method")).alias("num_methods"),
        F.round(F.avg("session_no_sum_30d"), 2).alias("avg_session_no_sum_30d"),
        F.size(F.collect_set("method")).alias("num_methods_2"),
    )
    .filter(F.col("cash_events") > 1)
)

result = (
    stats
    .select(
        "session_no_sum_30d_tier",
        "paypal_session_no_sum_30d",
        "cash_events",
        "avg_session_no_sum_30d",
        "num_methods_2",
    )
)
