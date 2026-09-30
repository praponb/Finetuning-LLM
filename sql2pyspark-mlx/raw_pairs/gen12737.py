from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device").orderBy("started_at", "id")
w2 = Window.partitionBy("device").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("browser").orderBy("started_at", "id")
w4 = Window.partitionBy("browser").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("device").orderBy("prev_started_at", "id")
w6 = Window.partitionBy("device").orderBy("prev_started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w7 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.duration_sec").desc(), F.col("c.id").desc())
w8 = Window.partitionBy("web_event_page").orderBy(F.col("session_no").desc(), "id")
w9 = Window.partitionBy("country", "customer_id").orderBy(F.col("session_no").desc())
w10 = Window.partitionBy("event_type")
w11 = Window.partitionBy("customer_id").orderBy(F.col("started_at").cast("bigint")).rangeBetween(-604800, Window.currentRow)
w12 = Window.orderBy("burst_id")
w13 = Window.partitionBy("device")

with_prev = df_sessions.withColumn("prev_started_at", F.lag("started_at").over(w))

with_gaps = (
    with_prev
    .withColumn("is_new_burst", F.when(F.col("prev_started_at").isNull() | (F.col("started_at") > F.col("prev_started_at") + F.expr("INTERVAL 30 DAYS")), 1).otherwise(0))
)

sessions_sessions = with_gaps.withColumn("burst_id", F.sum("is_new_burst").over(w2))

flagged = (
    sessions_sessions
    .withColumn("is_new_burst_2", F.when(F.lag("started_at").over(w3).isNull() | (F.col("started_at") > F.lag("started_at").over(w3) + F.expr("INTERVAL 14 DAYS")), 1).otherwise(0))
)

sessionized = flagged.withColumn("session_no", F.sum("is_new_burst_2").over(w4))

flagged_2 = (
    sessionized
    .withColumn("is_new_burst_3", F.when(F.col("prev_started_at") - F.lag("prev_started_at").over(w5) > F.expr("INTERVAL 90 DAYS"), 1).otherwise(0))
)

sessionized_sessions = (
    flagged_2
    .select(
        "id",
        "customer_id",
        "device",
        "started_at",
        "country",
        "prev_started_at",
        "burst_id",
        "is_new_burst_2",
        "session_no",
        F.sum("is_new_burst_3").over(w6).alias("session_no_2"),
    )
)

with_last = (
    sessionized_sessions
    .alias("s")
    .join(df_web_events.alias("c"), (F.col("c.session_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w7))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.page").alias("web_event_page"), F.col("c.event_type"))
)

ranked = (
    with_last
    .withColumn("rn", F.row_number().over(w8))
    .filter(F.col("rn") == 1)
)

remaining_keys = (
    ranked
    .select("country", "customer_id")
    .intersectAll(
        df_sessions
        .filter(F.col("country").isin(["US", "UK"]))
        .select("country", "customer_id")
    )
)

kept_rows = (
    ranked
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.country") == F.col("k.country")) & (F.col("s.customer_id") == F.col("k.customer_id")), "inner")
    .select("s.*")
)

scored = (
    kept_rows
    .filter(F.col("is_new_burst_2") == 1)
    .select(
        "id",
        "device",
        "burst_id",
        "session_no",
        "session_no_2",
        "rn",
        F.dense_rank().over(w9).alias("session_no_dense_rank"),
        F.max("is_new_burst_2").over(w10).alias("event_type_max_is_new_burst_2"),
        F.count("burst_id").over(w11).alias("burst_id_count_7d"),
        F.rank().over(w12).alias("burst_id_rank"),
        F.avg("session_no").over(w13).alias("device_avg_session_no"),
    )
)

result = (
    scored
    .cube("burst_id")
    .agg(
        F.size(F.collect_set("device")).alias("num_devices"),
        F.count("*").alias("event_count"),
    )
    .filter(F.col("num_devices") > 1)
)
