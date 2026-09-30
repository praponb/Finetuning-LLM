from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("session_id").orderBy("duration_sec")
w2 = Window.partitionBy("session_id").orderBy("event_time", "id")
w3 = Window.partitionBy("session_id").orderBy("event_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("session_id").orderBy("prev_event_time", "id")
w5 = Window.partitionBy("session_id").orderBy("prev_event_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

base = (
    df_web_events
    .filter((F.col("event_time") > F.lit("2023-05-22 14:00:00").cast("timestamp")) & F.col("page").isNotNull())
    .select(
        "id",
        "session_id",
        "event_time",
        "duration_sec",
        F.when(F.col("page") == "/home", 1).otherwise(0).alias("is_home"),
    )
)

ranked = (
    base
    .withColumn("dedup_rank", F.rank().over(w))
    .filter(F.col("dedup_rank") == 1)
)

with_prev = ranked.withColumn("prev_event_time", F.lag("event_time").over(w2))

with_gaps = (
    with_prev
    .withColumn("is_new_burst", F.when(F.col("prev_event_time").isNull() | (F.col("event_time") > F.col("prev_event_time") + F.expr("INTERVAL 14 DAYS")), 1).otherwise(0))
)

sessionized = with_gaps.withColumn("island_id", F.sum("is_new_burst").over(w3))

flagged = (
    sessionized
    .withColumn("is_new_burst_2", F.when(F.lag("prev_event_time").over(w4).isNull() | (F.col("prev_event_time") > F.lag("prev_event_time").over(w4) + F.expr("INTERVAL 7 DAYS")), 1).otherwise(0))
)

sessions_web_events = (
    flagged
    .select(
        "id",
        "session_id",
        "dedup_rank",
        "prev_event_time",
        "is_new_burst",
        "island_id",
        F.sum("is_new_burst_2").over(w5).alias("island_id_2"),
    )
)

enriched = (
    sessions_web_events
    .alias("s")
    .join(df_sessions.alias("p"), F.col("s.session_id") == F.col("p.id"), "left")
    .filter(F.col("p.customer_id").isNotNull())
    .select(
        "s.id",
        "s.session_id",
        "s.dedup_rank",
        "s.island_id",
        F.col("p.country"),
        F.col("p.browser"),
    )
)

result = (
    enriched
    .select("id", "dedup_rank", "country", "browser")
    .orderBy(F.col("browser").desc())
)
