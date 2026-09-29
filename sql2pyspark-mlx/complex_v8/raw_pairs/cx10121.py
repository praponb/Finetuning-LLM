from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_source = Window.partitionBy("source").orderBy("played_at", "id")
w_source_running = Window.partitionBy("source").orderBy("played_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

scoped = df_plays

with_prev = scoped.withColumn("prev_played_at", F.lag("played_at").over(w_source))

flagged = (
    with_prev
    .withColumn("is_new_session", F.when(F.col("prev_played_at").isNull() | ((F.unix_timestamp("played_at") - F.unix_timestamp("prev_played_at")) / 60 > 45), 1).otherwise(0))
)

sessionized = flagged.withColumn("session_no", F.sum("is_new_session").over(w_source_running))

session_summary = (
    sessionized
    .groupBy("source", "session_no")
    .agg(
        F.min("played_at").alias("session_start"),
        F.max("played_at").alias("session_end"),
        F.count("*").alias("n_events"),
        F.round((F.unix_timestamp(F.max("played_at")) - F.unix_timestamp(F.min("played_at"))) / 60, 1).alias("duration_min"),
    )
)

without_matches = (
    session_summary
    .crossJoin(session_summary.agg(F.avg("duration_min").alias("overall_avg_duration_min")))
    .filter(F.col("duration_min") > F.col("overall_avg_duration_min"))
    .drop("overall_avg_duration_min")
)

qualified = (
    without_matches
    .crossJoin(without_matches.agg(F.avg("duration_min").alias("overall_avg_duration_min")))
    .filter(F.col("duration_min") > F.col("overall_avg_duration_min"))
    .drop("overall_avg_duration_min")
)

result = (
    qualified
    .select(
        "source",
        "session_no",
        "session_start",
        "session_end",
        "n_events",
        "duration_min",
        F.round(F.col("session_no") / F.nullif(F.col("duration_min"), F.lit(0)), 3).alias("session_no_per_duration_min"),
        F.round(F.col("duration_min") * 1.5, 2).alias("duration_min_adj"),
    )
    .orderBy(F.col("session_end").desc())
)
