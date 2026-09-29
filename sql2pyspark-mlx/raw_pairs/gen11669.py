from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("event_type").orderBy("event_time", "id")
w2 = Window.partitionBy("event_type").orderBy("event_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("session_id").orderBy(F.col("event_time").asc_nulls_last(), "id")
w4 = Window.partitionBy("session_browser").orderBy("event_time", "id")
w5 = Window.partitionBy("session_browser").orderBy("event_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
duration_sec_by_session_id = df_web_events.groupBy("session_id").agg(F.avg("duration_sec").alias("avg_web_event_duration_sec"))

with_gaps = (
    df_web_events
    .withColumn("is_new_burst", F.when(F.lag("event_time").over(w).isNull() | (F.col("event_time") > F.lag("event_time").over(w) + F.expr("INTERVAL 90 DAYS")), 1).otherwise(0))
)

islands = with_gaps.withColumn("island_id", F.sum("is_new_burst").over(w2))

valid = (
    islands
    .filter(F.col("session_id") == 2)
    .select(
        "*",
        F.coalesce("page", F.lit("unassigned")).alias("page_filled"),
        F.least("duration_sec", "is_new_burst").alias("least_duration_sec_is_new_burst"),
        F.coalesce("page", F.lit("NONE")).alias("page_filled_2"),
    )
)

enriched = (
    valid
    .alias("src")
    .join(df_sessions.alias("d"), F.col("src.session_id") == F.col("d.id"), "left")
    .filter(F.col("d.country").isNotNull())
    .select(
        "src.id",
        "src.session_id",
        "src.page",
        "src.event_time",
        "src.island_id",
        F.col("d.browser"),
        F.col("d.country"),
    )
)

with_dims = (
    enriched
    .alias("s")
    .join(df_sessions.alias("p2"), F.col("s.session_id") == F.col("p2.id"), "left")
    .select(
        "s.id",
        "s.session_id",
        "s.event_time",
        "s.browser",
        "s.country",
        F.col("p2.device"),
        F.col("p2.country").alias("session_country"),
        F.col("p2.browser").alias("session_browser"),
    )
)

deduped = (
    with_dims
    .withColumn("rn", F.row_number().over(w3))
    .filter(F.col("rn") == 1)
)

flagged = (
    deduped
    .withColumn("session_start_flag", F.when(F.lag("event_time").over(w4).isNull() | (F.col("event_time") > F.lag("event_time").over(w4) + F.expr("INTERVAL 30 DAYS")), 1).otherwise(0))
)

islands_web_events = (
    flagged
    .select(
        "id",
        "session_id",
        "event_time",
        "browser",
        "session_country",
        "session_browser",
        F.sum("session_start_flag").over(w5).alias("island_id"),
    )
)

result = (
    islands_web_events
    .join(duration_sec_by_session_id, on="session_id", how="left")
    .select(
        "id",
        "session_id",
        "event_time",
        "browser",
        "session_country",
        F.when(F.col("browser").rlike("^(Firefox|Chrome)$"), "GROUP_B").otherwise("OTHER_ZONE").alias("browser_zone"),
        F.col("avg_web_event_duration_sec"),
    )
    .orderBy("browser")
)
