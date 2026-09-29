from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("session_id").orderBy(F.col("event_time").desc_nulls_last(), "id")
w2 = Window.partitionBy("device").orderBy("event_time", "id")
w3 = Window.partitionBy("device").orderBy("event_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("country").orderBy(F.col("is_new_session").desc_nulls_last())
duration_sec_by_session_id = df_web_events.groupBy("session_id").agg(F.max("duration_sec").alias("max_web_event_duration_sec"))

with_dims = (
    df_web_events
    .alias("e")
    .join(df_sessions.alias("s"), F.col("e.session_id") == F.col("s.id"), "inner")
    .select("e.*", F.col("s.country"))
)

ranked = (
    with_dims
    .withColumn("rank_in_group", F.row_number().over(w))
    .filter(F.col("rank_in_group") == 1)
)

joined = (
    ranked
    .alias("s")
    .join(df_sessions.alias("d"), F.col("s.session_id") == F.col("d.id"), "left")
    .filter(F.col("d.browser") == "Firefox")
    .select(
        "s.id",
        "s.session_id",
        "s.event_type",
        "s.event_time",
        "s.duration_sec",
        "s.country",
        F.col("d.device"),
    )
)

flagged = (
    joined
    .withColumn("is_new_session", F.when(F.lag("event_time").over(w2).isNull() | (F.col("event_time") > F.lag("event_time").over(w2) + F.expr("INTERVAL 60 DAYS")), 1).otherwise(0))
)

sessions_web_events = flagged.withColumn("session_no", F.sum("is_new_session").over(w3))

latest = (
    sessions_web_events
    .withColumn("rank_in_group", F.rank().over(w4))
    .filter(F.col("rank_in_group") == 1)
    .select(
        "id",
        "session_id",
        "event_type",
        "event_time",
        "duration_sec",
        "country",
        "is_new_session",
        "session_no",
    )
)

result = (
    latest
    .join(duration_sec_by_session_id, on="session_id", how="left")
    .select(
        "id",
        "session_id",
        "event_type",
        "event_time",
        "duration_sec",
        "country",
        "is_new_session",
        "session_no",
        F.when(F.col("country").rlike("^(SG|UK)$"), "OTHER_ZONE").otherwise("EXTENDED").alias("country_zone"),
        F.col("max_web_event_duration_sec"),
    )
    .orderBy(F.col("id").desc_nulls_last(), F.col("session_no").desc())
    .limit(5)
)
