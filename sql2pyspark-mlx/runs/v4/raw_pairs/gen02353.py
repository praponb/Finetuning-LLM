from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.event_time").desc(), F.col("c.id").desc())
w2 = Window.partitionBy("country").orderBy("web_event_event_time", "id")
w3 = Window.partitionBy("country").orderBy("web_event_event_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("device").orderBy("web_event_event_time", "id")
w5 = Window.partitionBy("device").orderBy("web_event_event_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

base = (
    df_sessions
    .filter(F.col("country").isin(["JP", "US"]))
    .select(
        "id",
        "customer_id",
        "device",
        "browser",
        "started_at",
        "country",
        F.dayofweek("started_at").alias("started_at_dayofweek"),
        F.substring("browser", 1, 3).alias("browser_prefix"),
    )
)

with_latest = (
    base
    .alias("s")
    .join(df_web_events.alias("c"), (F.col("c.session_id") == F.col("s.id")) & (F.col("c.event_time") <= F.col("s.started_at")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select(
        "s.*",
        F.col("c.event_time").alias("web_event_event_time"),
        F.col("c.duration_sec"),
    )
)

gaps = (
    with_latest
    .withColumn("session_start_flag", F.when(F.lag("web_event_event_time").over(w2).isNull() | (F.col("web_event_event_time") > F.lag("web_event_event_time").over(w2) + F.expr("INTERVAL 14 DAYS")), 1).otherwise(0))
)

sessionized = gaps.withColumn("burst_id", F.sum("session_start_flag").over(w3))

prev_events = (
    sessionized
    .withColumn("prev_web_event_event_time", F.lag("web_event_event_time").over(w4))
)

flagged = (
    prev_events
    .withColumn("session_start_flag_2", F.when(F.col("prev_web_event_event_time").isNull() | (F.col("web_event_event_time") > F.col("prev_web_event_event_time") + F.expr("INTERVAL 30 DAYS")), 1).otherwise(0))
)

sessionized_sessions = flagged.withColumn("burst_id_2", F.sum("session_start_flag_2").over(w5))

result = (
    sessionized_sessions
    .select(
        "id",
        "device",
        "started_at",
        "country",
        "browser_prefix",
        "web_event_event_time",
        "duration_sec",
        "burst_id",
        "prev_web_event_event_time",
        F.when(F.col("device").rlike("^(desktop|tablet)$"), "CORE").otherwise("OTHER_ZONE").alias("device_zone"),
    )
)
