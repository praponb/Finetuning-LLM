from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device").orderBy("started_at", "id")
w2 = Window.partitionBy("device").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("device").orderBy(F.col("session_no").desc())
w4 = Window.partitionBy("country").orderBy("started_at", "id")
w5 = Window.partitionBy("country").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.partitionBy("device").orderBy("started_at", "id").rowsBetween(-3, Window.currentRow)
w7 = Window.partitionBy("device").orderBy("burst_id", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

enriched = (
    df_sessions
    .alias("a")
    .join(df_customers.alias("d"), F.col("a.customer_id") == F.col("d.id"), "left")
    .select(
        "a.id",
        "a.customer_id",
        "a.device",
        "a.browser",
        "a.started_at",
        "a.country",
        F.col("d.segment"),
    )
)

with_prev = enriched.withColumn("prev_started_at", F.lag("started_at").over(w))

gaps = (
    with_prev
    .withColumn("session_start_flag", F.when(F.col("prev_started_at").isNull() | (F.col("started_at") > F.col("prev_started_at") + F.expr("INTERVAL 90 DAYS")), 1).otherwise(0))
)

islands = (
    gaps
    .select(
        "id",
        "device",
        "started_at",
        "country",
        "segment",
        F.sum("session_start_flag").over(w2).alias("session_no"),
    )
)

ranked = (
    islands
    .withColumn("rank_in_group", F.rank().over(w3))
    .filter(F.col("rank_in_group") == 1)
    .drop("rank_in_group")
)

with_gaps = (
    ranked
    .withColumn("session_start_flag", F.when(F.col("started_at") - F.lag("started_at").over(w4) > F.expr("INTERVAL 60 DAYS"), 1).otherwise(0))
)

sessionized = (
    with_gaps
    .select(
        "id",
        "device",
        "started_at",
        "country",
        "session_no",
        F.sum("session_start_flag").over(w5).alias("burst_id"),
    )
)

metrics = (
    sessionized
    .select(
        "id",
        "started_at",
        "session_no",
        "burst_id",
        F.min("session_no").over(w6).alias("last4_min_session_no"),
        F.sum("session_no").over(w6).alias("last4_sum_session_no"),
        F.avg("session_no").over(w6).alias("last4_avg_session_no"),
        F.sum("session_no").over(w7).alias("running_session_no"),
    )
)

result = (
    metrics
    .select(
        "id",
        "session_no",
        "burst_id",
        "last4_min_session_no",
        "last4_avg_session_no",
        "running_session_no",
    )
    .orderBy(F.col("session_no").desc())
)
