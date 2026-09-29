from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("session_id").orderBy("event_time", "id")
w2 = Window.partitionBy("session_id").orderBy("event_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("session_id", "event_type").orderBy(F.col("session_id_2").desc())

flagged = (
    df_web_events
    .withColumn("is_new_session", F.when(F.lag("event_time").over(w).isNull() | (F.col("event_time") > F.lag("event_time").over(w) + F.expr("INTERVAL 30 DAYS")), 1).otherwise(0))
)

sessionized = flagged.withColumn("session_id_2", F.sum("is_new_session").over(w2))

enriched = (
    sessionized
    .filter(F.col("duration_sec").isNotNull())
    .select(
        "id",
        "session_id",
        "event_type",
        "duration_sec",
        "session_id_2",
        F.min("session_id_2").over(w2).alias("running_min_session_id_2"),
        F.sum("session_id_2").over(w2).alias("running_sum_session_id_2"),
        F.count("session_id_2").over(w2).alias("running_count_session_id_2"),
        F.max("session_id_2").over(w2).alias("running_max_session_id_2"),
    )
)

ranked = (
    enriched
    .withColumn("latest_rank", F.rank().over(w3))
    .filter(F.col("latest_rank") == 1)
    .select(
        "id",
        "session_id",
        "duration_sec",
        "session_id_2",
        "running_count_session_id_2",
    )
)

valid = (
    ranked
    .filter(F.col("duration_sec") >= 368)
    .withColumn("duration_sec_times_session_id_2", (F.col("duration_sec") * F.col("session_id_2")))
    .withColumn("running_count_session_id_2_str", F.col("running_count_session_id_2").cast("string"))
)

with_dims = (
    valid
    .alias("e")
    .join(df_sessions.alias("b"), F.col("e.session_id") == F.col("b.id"), "inner")
    .select("e.*", F.col("b.browser"), F.col("b.country"))
)

result = (
    with_dims
    .select(
        "id",
        "session_id",
        "running_count_session_id_2",
        "duration_sec_times_session_id_2",
        "browser",
        "country",
    )
    .orderBy(F.col("duration_sec_times_session_id_2").asc_nulls_first())
)
