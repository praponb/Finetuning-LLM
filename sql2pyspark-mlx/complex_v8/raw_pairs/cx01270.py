from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_source = Window.partitionBy("source").orderBy("played_at", "id")
w_source_running = Window.partitionBy("source").orderBy("played_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

filtered = df_plays.filter(F.col("source") == "album")

with_prev = filtered.withColumn("prev_played_at", F.lag("played_at").over(w_source))

flagged = (
    with_prev
    .withColumn("new_session_flag", F.when(F.col("prev_played_at").isNull() | (F.col("played_at") > F.col("prev_played_at") + F.expr("INTERVAL 30 MINUTES")), 1).otherwise(0))
)

sessionized = flagged.withColumn("session_no", F.sum("new_session_flag").over(w_source_running))

sessions = (
    sessionized
    .groupBy("source", "session_no")
    .agg(
        F.min("played_at").alias("session_start"),
        F.max("played_at").alias("session_end"),
        F.count("*").alias("n_events"),
        F.sum("ms_played").alias("session_ms_played"),
        F.round((F.unix_timestamp(F.max("played_at")) - F.unix_timestamp(F.min("played_at"))) / 60, 1).alias("duration_min"),
    )
)

tagged = (
    sessions
    .select(
        "*",
        F.substring(F.trim(F.col("source")), 1, 4).alias("source_prefix"),
        F.lower(F.regexp_replace(F.trim(F.col("source")), " ", "_")).alias("source_slug"),
    )
)

result = (
    tagged
    .select("source_prefix", "session_ms_played", "session_start")
    .distinct()
    .orderBy(F.col("session_ms_played").desc())
)
