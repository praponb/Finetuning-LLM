from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_track_id_by_ms_played = Window.partitionBy("track_id").orderBy(F.col("ms_played").desc(), "id")
w_listener_id = Window.partitionBy("listener_id")
w_listener_id_track_id_by_ms_p = Window.partitionBy("listener_id", "track_id").orderBy(F.col("ms_played").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_listener_id_track_id_by_ms_p_2 = Window.partitionBy("listener_id", "track_id").orderBy(F.col("ms_played").desc())
w_track_id_by_ms_played_ties = Window.partitionBy("track_id").orderBy(F.col("ms_played").desc())
w_listener_id_track_id_by_ms_p_3 = Window.partitionBy("listener_id", "track_id").orderBy(F.col("ms_played").desc(), "id")

base_plays = (
    df_plays
    .filter((F.col("is_skipped")) & (F.col("device") == "tv"))
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "ms_played",
        "device",
        "is_skipped",
        "source",
        F.coalesce(F.col("source"), F.lit("UNKNOWN")).alias("source_or_unknown"),
    )
)

with_details = (
    base_plays.alias("s")
    .join(df_listeners.alias("l"), (F.col("l.id") == F.col("s.listener_id")) & (F.upper(F.trim(F.col("l.username"))).like("H%")), "inner")
    .filter(F.col("l.country").isin("KR", "UK"))
    .select(
        "s.id",
        "s.listener_id",
        "s.track_id",
        "s.played_at",
        "s.ms_played",
        "s.is_skipped",
        "s.source_or_unknown",
        F.col("l.country").alias("listener_country"),
        F.col("l.interests").alias("listener_interests"),
    )
)

metrics = (
    with_details
    .select(
        "id",
        "played_at",
        "is_skipped",
        "source_or_unknown",
        "listener_country",
        F.round(F.percent_rank().over(w_track_id_by_ms_played), 3).alias("ms_played_percent_rank"),
        F.round(F.col("ms_played") / F.nullif(F.sum("ms_played").over(w_listener_id), F.lit(0)), 4).alias("ms_played_share_of_listener_id"),
        F.first("ms_played", ignorenulls=True).over(w_listener_id_track_id_by_ms_p).alias("first_known_ms_played"),
        F.round(F.cume_dist().over(w_listener_id_track_id_by_ms_p_2), 3).alias("ms_played_cume_dist"),
        F.max("ms_played").over(w_listener_id).alias("listener_id_max_ms_played"),
        F.count(F.lit(1)).over(w_listener_id).alias("rows_in_listener_id"),
        F.sum("ms_played").over(w_listener_id).alias("listener_id_sum_ms_played"),
        F.row_number().over(w_track_id_by_ms_played).alias("rn"),
        F.round(F.percent_rank().over(w_track_id_by_ms_played_ties), 3).alias("ms_played_percent_rank_2"),
        F.round(F.cume_dist().over(w_listener_id_track_id_by_ms_p_3), 3).alias("ms_played_cume_dist_2"),
    )
)

result = (
    metrics
    .select(
        "id",
        "played_at",
        "is_skipped",
        "source_or_unknown",
        "ms_played_share_of_listener_id",
        "ms_played_cume_dist",
        "listener_id_max_ms_played",
        "rows_in_listener_id",
        "listener_id_sum_ms_played",
        "rn",
        "ms_played_percent_rank_2",
        "ms_played_cume_dist_2",
        F.lower(F.regexp_replace(F.trim(F.col("listener_country")), " ", "_")).alias("listener_country_slug"),
    )
    .orderBy(F.col("played_at").desc(), "id")
    .limit(5)
)
