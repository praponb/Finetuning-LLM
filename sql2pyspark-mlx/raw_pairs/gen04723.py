from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device").orderBy("started_at", "id")
w2 = Window.partitionBy("device").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy(F.month("started_at"), "device")
w4 = Window.partitionBy("country").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("country").orderBy("session_no")
w6 = Window.partitionBy(F.to_date("started_at")).orderBy(F.col("session_no").desc(), "id")
w7 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.duration_sec").desc(), F.col("c.id").desc())

kept = (
    df_sessions
    .alias("s")
    .join(df_web_events.alias("x"), (F.col("x.session_id") == F.col("s.id")) & (F.col("x.session_id") == 22), "left_anti")
)

eligible = (
    kept
    .alias("s")
    .join(df_web_events.alias("ch"), (F.col("ch.session_id") == F.col("s.id")) & (F.col("ch.event_time") >= F.col("s.started_at")), "left_semi")
)

with_gaps = (
    eligible
    .withColumn("is_new_burst", F.when(F.col("started_at") - F.lag("started_at").over(w) > F.expr("INTERVAL 60 DAYS"), 1).otherwise(0))
)

sessions_sessions = (
    with_gaps
    .select(
        "id",
        "device",
        "browser",
        "started_at",
        "country",
        F.sum("is_new_burst").over(w2).alias("session_no"),
    )
)

with_windows = (
    sessions_sessions
    .select(
        "*",
        F.count("session_no").over(w3).alias("started_at_month_count_session_no"),
        F.sum("session_no").over(w4).alias("running_session_no"),
        F.rank().over(w5).alias("session_no_rank"),
        F.ntile(4).over(w6).alias("session_no_bucket"),
    )
)

with_latest = (
    with_windows
    .alias("s")
    .join(df_web_events.alias("c"), (F.col("c.session_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w7))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.device",
        "s.country",
        "s.started_at_month_count_session_no",
        "s.session_no_rank",
        "s.session_no_bucket",
        F.col("c.event_time").alias("web_event_event_time"),
    )
)

remaining_keys = (
    with_latest
    .select("device", "id")
    .subtract(
        df_sessions
        .filter(F.hour("started_at").between(10, 15))
        .select("device", "id")
    )
)

reconciled = (
    with_latest
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.device") == F.col("k.device")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

result = (
    reconciled
    .select("id", "device", "web_event_event_time")
    .orderBy(F.col("device").asc_nulls_last())
)
