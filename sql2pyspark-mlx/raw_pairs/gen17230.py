from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("session_id").orderBy("event_time", "id")
w2 = Window.partitionBy("session_id").orderBy("event_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("page").orderBy("event_time", "id")
w4 = Window.partitionBy("page").orderBy("event_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
duration_sec_by_session_id = df_web_events.groupBy("session_id").agg(F.avg("duration_sec").alias("avg_web_event_duration_sec"))

cleaned = (
    df_web_events
    .filter((F.col("event_time") >= F.lit("2023-06-28").cast("date")) & F.col("page").isNotNull())
    .select(
        "id",
        "session_id",
        "event_type",
        "page",
        "event_time",
        F.concat("page", F.lit(" - "), "event_type").alias("page_event_type"),
        F.trim("event_type").alias("clean_event_type"),
    )
)

flagged = (
    cleaned
    .withColumn("is_new_session", F.when(F.col("event_time") - F.lag("event_time").over(w) > F.expr("INTERVAL 90 DAYS"), 1).otherwise(0))
)

islands = flagged.withColumn("session_id_2", F.sum("is_new_session").over(w2))

flagged_2 = (
    islands
    .withColumn("is_new_burst", F.when(F.lag("event_time").over(w3).isNull() | (F.col("event_time") > F.lag("event_time").over(w3) + F.expr("INTERVAL 90 DAYS")), 1).otherwise(0))
)

sessionized = (
    flagged_2
    .select(
        "id",
        "session_id",
        "event_type",
        "page",
        "event_time",
        "page_event_type",
        "clean_event_type",
        "is_new_session",
        F.sum("is_new_burst").over(w4).alias("session_id_3"),
    )
)

kept_keys = (
    sessionized
    .select("session_id", "event_type")
    .subtract(
        df_web_events
        .filter(F.col("page") == "/cart")
        .select("session_id", "event_type")
    )
)

verified = (
    sessionized
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.session_id") == F.col("k.session_id")) & (F.col("s.event_type") == F.col("k.event_type")), "inner")
    .select("s.*")
)

result = (
    verified
    .join(duration_sec_by_session_id, on="session_id", how="left")
    .select(
        "session_id",
        "event_type",
        "page",
        "page_event_type",
        "is_new_session",
        "session_id_3",
        F.col("avg_web_event_duration_sec"),
    )
    .orderBy(F.col("avg_web_event_duration_sec").desc_nulls_last(), "session_id")
)
