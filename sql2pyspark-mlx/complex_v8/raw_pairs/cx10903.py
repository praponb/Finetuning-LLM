from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_source_by_played_at = Window.partitionBy("source").orderBy(F.col("played_at").desc(), "id")
w_track_id_listener_id_by_play = Window.partitionBy("track_id", "listener_id").orderBy(F.col("played_at").desc(), "id")
w_track_is_explicit = Window.partitionBy("track_is_explicit")
w_listener_id = Window.partitionBy("listener_id")
w_track_id_played_at_year = Window.partitionBy("track_id", F.year("played_at"))
w_track_is_explicit_track_id_b = Window.partitionBy("track_is_explicit", "track_id").orderBy(F.col("track_release_date").desc_nulls_first(), "id")
w_track_id_listener_id_by_play_2 = Window.partitionBy("track_id", "listener_id").orderBy(F.col("played_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

base_plays = (
    df_plays
    .filter(F.col("ms_played") < 245815)
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "ms_played",
        "source",
    )
)

with_parent_listener = (
    base_plays.alias("s")
    .join(df_listeners.alias("l"), F.col("l.id") == F.col("s.listener_id"), "left")
    .join(df_tracks.alias("t"), F.col("t.id") == F.col("s.track_id"), "inner")
    .select(
        "s.id",
        "s.listener_id",
        "s.track_id",
        "s.played_at",
        "s.ms_played",
        "s.source",
        F.col("l.interests").alias("listener_interests"),
        F.col("t.is_explicit").alias("track_is_explicit"),
        F.col("t.release_date").alias("track_release_date"),
    )
)

metrics = (
    with_parent_listener
    .select(
        "id",
        "listener_id",
        "track_id",
        "ms_played",
        "listener_interests",
        "track_is_explicit",
        F.lag("track_release_date").over(w_source_by_played_at).alias("prev_track_release_date"),
        F.datediff(F.col("track_release_date"), F.lag("track_release_date").over(w_track_id_listener_id_by_play)).alias("days_since_prev_track_release_date"),
        F.round(F.col("ms_played") / F.nullif(F.sum("ms_played").over(w_track_is_explicit), F.lit(0)), 4).alias("ms_played_share_of_track_is_explicit"),
        F.size(F.collect_set("source").over(w_listener_id)).alias("distinct_source_in_listener_id"),
        F.lead("ms_played", 1, 0).over(w_track_id_listener_id_by_play).alias("next_ms_played"),
        F.round(F.col("ms_played") - F.avg("ms_played").over(w_track_id_played_at_year), 2).alias("ms_played_vs_track_id_played_at_year_avg"),
        F.round(F.col("ms_played") - F.avg("ms_played").over(w_listener_id), 2).alias("ms_played_vs_listener_id_avg"),
        F.sum("ms_played").over(w_listener_id).alias("listener_id_sum_ms_played"),
        F.ntile(10).over(w_track_is_explicit_track_id_b).alias("track_release_date_decile"),
        F.min("ms_played").over(w_track_id_listener_id_by_play_2).alias("min_ms_played_so_far"),
    )
)

result = (
    metrics
    .filter(F.col("min_ms_played_so_far") < 3201000)
    .select(
        "id",
        "listener_id",
        "track_id",
        "listener_interests",
        "track_is_explicit",
        "prev_track_release_date",
        "days_since_prev_track_release_date",
        "distinct_source_in_listener_id",
        "next_ms_played",
        "ms_played_vs_track_id_played_at_year_avg",
        "track_release_date_decile",
    )
    .orderBy(F.col("distinct_source_in_listener_id").desc_nulls_first(), "id")
    .limit(20)
)
