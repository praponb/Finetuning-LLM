from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_track_id = Window.partitionBy("track_id").orderBy("played_at", "id")
w_track_id_running = Window.partitionBy("track_id").orderBy("played_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_dedup = Window.partitionBy("source", "track_id").orderBy(F.col("session_id").desc(), F.col("id").desc())

recent = df_plays

with_prev = recent.withColumn("prev_played_at", F.lag("played_at").over(w_track_id))

flagged = (
    with_prev
    .withColumn("new_session_flag", F.when(F.col("prev_played_at").isNull() | (F.col("played_at") > F.col("prev_played_at") + F.expr("INTERVAL 60 MINUTES")), 1).otherwise(0))
)

sessionized = flagged.withColumn("session_id", F.sum("new_session_flag").over(w_track_id_running))

latest = (
    sessionized
    .withColumn("row_num", F.row_number().over(w_dedup))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

result = (
    latest
    .select(
        "id",
        "listener_id",
        "track_id",
        "ms_played",
        "is_skipped",
        "source",
        "prev_played_at",
        "new_session_flag",
        F.round(F.col("session_id") / F.nullif(F.col("new_session_flag"), F.lit(0)), 3).alias("session_id_per_new_session_flag"),
        F.to_date(F.col("prev_played_at")).alias("prev_played_at_date"),
    )
    .orderBy("listener_id")
)
