from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("method").orderBy("paid_at", "id")
w2 = Window.partitionBy("method").orderBy("paid_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("status").orderBy("paid_at", "id")
w4 = Window.partitionBy("status").orderBy("paid_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy(F.date_trunc("month", "paid_at"), "method").orderBy(F.col("is_new_session").asc_nulls_first())
w_running = Window.orderBy(F.col("paid_at").cast("bigint")).rangeBetween(-7776000, Window.currentRow)
w7 = Window.partitionBy("method")
w8 = Window.partitionBy("status", "method")

valid = (
    df_payments
    .filter(F.col("order_id").isNotNull())
    .select(
        "id",
        "method",
        "paid_at",
        "status",
        F.ceil("amount").alias("amount_whole"),
        F.length("status").alias("status_length"),
    )
)

with_prev = valid.withColumn("prev_paid_at", F.lag("paid_at").over(w))

with_gaps = (
    with_prev
    .withColumn("is_new_burst", F.when(F.col("prev_paid_at").isNull() | (F.col("paid_at") > F.col("prev_paid_at") + F.expr("INTERVAL 14 DAYS")), 1).otherwise(0))
)

islands = (
    with_gaps
    .select(
        "id",
        "method",
        "paid_at",
        "status",
        "amount_whole",
        "status_length",
        F.sum("is_new_burst").over(w2).alias("burst_id"),
    )
)

with_gaps_2 = (
    islands
    .withColumn("is_new_session", F.when(F.lag("paid_at").over(w3).isNull() | (F.col("paid_at") > F.lag("paid_at").over(w3) + F.expr("INTERVAL 7 DAYS")), 1).otherwise(0))
)

sessionized = with_gaps_2.withColumn("island_id", F.sum("is_new_session").over(w4))

matched_keys = (
    sessionized
    .select("id")
    .intersectAll(
        df_payments
        .filter(F.col("method").isNotNull())
        .select("id")
    )
)

verified = (
    sessionized
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

metrics = (
    verified
    .select(
        "*",
        F.dense_rank().over(w5).alias("is_new_session_dense_rank"),
        F.sum("island_id").over(w_running).alias("island_id_sum_90d"),
        F.max("burst_id").over(w7).alias("method_max_burst_id"),
        F.min("status_length").over(w8).alias("status_min_status_length"),
    )
)

result = (
    metrics
    .select(
        "method",
        "paid_at",
        "status",
        "amount_whole",
        "status_length",
        "island_id",
        "island_id_sum_90d",
        "status_min_status_length",
        F.when(F.col("status").rlike("^(FAILED|REFUNDED)$"), "GROUP_A").otherwise("EXTENDED").alias("status_zone"),
    )
    .orderBy("island_id")
)
