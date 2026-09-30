from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_release_date_prior_2 = Window.orderBy("release_date", "id").rowsBetween(-2, -1)
w_artist_id_is_explicit = Window.partitionBy("artist_id", "is_explicit")
w_all_by_release_date_running = Window.orderBy("release_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_play_played_at_year = Window.partitionBy("play_played_at_year")
w_all_by_avg_play_ms_played_fu = Window.orderBy(F.col("avg_play_ms_played").desc(), "play_played_at_year", "release_date_ym").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_avg_play_ms_played_mo = Window.orderBy(F.col("avg_play_ms_played").desc(), "play_played_at_year", "release_date_ym").rowsBetween(-3, Window.currentRow)
w_play_played_at_year_by_relea = Window.partitionBy("play_played_at_year").orderBy("release_date_span_days_vs_play_played_at_year_av")
w_play_played_at_year_by_relea_2 = Window.partitionBy("play_played_at_year").orderBy("release_date_span_days_vs_play_played_at_year_av", "play_played_at_year", "release_date_ym").rowsBetween(Window.unboundedPreceding, Window.currentRow)

scoped = (
    df_tracks
    .filter(F.col("duration_sec").between(183, 352))
    .select(
        "id",
        "artist_id",
        "title",
        "release_date",
        "is_explicit",
        "bpm",
        "moods",
        F.month(F.col("release_date")).alias("release_date_month"),
    )
)

windowed = (
    scoped
    .select(
        "id",
        "artist_id",
        "release_date",
        "is_explicit",
        "moods",
        "release_date_month",
        F.round(F.avg("release_date_month").over(w_all_by_release_date_prior_2), 2).alias("release_date_month_avg_2_prior"),
        F.max("bpm").over(w_artist_id_is_explicit).alias("artist_id_is_explicit_max_bpm"),
        F.round(F.col("bpm") - F.avg("bpm").over(w_artist_id_is_explicit), 2).alias("bpm_vs_artist_id_is_explicit_avg"),
        F.first("release_date_month", ignorenulls=True).over(w_all_by_release_date_running).alias("first_known_release_date_month"),
    )
)

with_children_plays = (
    windowed.alias("s")
    .join(df_plays.alias("p"), F.col("p.track_id") == F.col("s.id"), "inner")
    .select(
        "s.id",
        "s.artist_id",
        "s.release_date",
        "s.is_explicit",
        "s.moods",
        "s.release_date_month",
        "s.release_date_month_avg_2_prior",
        "s.artist_id_is_explicit_max_bpm",
        "s.bpm_vs_artist_id_is_explicit_avg",
        "s.first_known_release_date_month",
        F.col("p.id").alias("play_id"),
        F.col("p.listener_id").alias("play_listener_id"),
        F.col("p.ms_played").alias("play_ms_played"),
        F.col("p.is_skipped").alias("play_is_skipped"),
        F.col("p.played_at").alias("play_played_at"),
    )
)

summary = (
    with_children_plays
    .groupBy(
        F.year("play_played_at").alias("play_played_at_year"),
        F.date_format("release_date", "yyyy-MM").alias("release_date_ym"),
    )
    .agg(
        F.round(F.avg("play_ms_played"), 2).alias("avg_play_ms_played"),
        F.datediff(F.max("release_date"), F.min("release_date")).alias("release_date_span_days"),
    )
)

windowed_2 = (
    summary
    .select(
        "*",
        F.last("release_date_span_days").over(w_all_by_avg_play_ms_played_fu).alias("last_release_date_span_days"),
        F.round(F.avg("play_played_at_year").over(w_play_played_at_year), 2).alias("play_played_at_year_avg_play_played_at_year"),
        F.max("play_played_at_year").over(w_all_by_avg_play_ms_played_mo).alias("play_played_at_year_max_4"),
        F.round(F.col("release_date_span_days") - F.avg("release_date_span_days").over(w_play_played_at_year), 2).alias("release_date_span_days_vs_play_played_at_year_av"),
        F.max("avg_play_ms_played").over(w_all_by_avg_play_ms_played_mo).alias("avg_play_ms_played_max_4"),
    )
)

metrics = (
    windowed_2
    .select(
        "play_played_at_year",
        "release_date_ym",
        "avg_play_ms_played",
        "release_date_span_days",
        "last_release_date_span_days",
        "play_played_at_year_avg_play_played_at_year",
        "play_played_at_year_max_4",
        "release_date_span_days_vs_play_played_at_year_av",
        "avg_play_ms_played_max_4",
        F.sum("avg_play_ms_played").over(w_play_played_at_year).alias("play_played_at_year_sum_avg_play_ms_played"),
        F.round(F.percent_rank().over(w_play_played_at_year_by_relea), 3).alias("release_date_span_days_vs_play_played_at_year_av_2"),
        F.min("play_played_at_year_max_4").over(w_play_played_at_year_by_relea_2).alias("min_play_played_at_year_max_4_so_far"),
        F.round(F.col("avg_play_ms_played_max_4") / F.nullif(F.sum("avg_play_ms_played_max_4").over(w_play_played_at_year), F.lit(0)), 4).alias("avg_play_ms_played_max_4_share_of_play_played_at"),
        F.count(F.lit(1)).over(w_play_played_at_year).alias("rows_in_play_played_at_year"),
    )
)

result = (
    metrics
    .select(
        "play_played_at_year",
        "release_date_ym",
        "release_date_span_days_vs_play_played_at_year_av_2",
        "release_date_span_days_vs_play_played_at_year_av",
        "play_played_at_year_sum_avg_play_ms_played",
        "avg_play_ms_played",
    )
    .unpivot(
        ids=["play_played_at_year", "release_date_ym"],
        values=["release_date_span_days_vs_play_played_at_year_av_2", "release_date_span_days_vs_play_played_at_year_av", "play_played_at_year_sum_avg_play_ms_played", "avg_play_ms_played"],
        variableColumnName="metric",
        valueColumnName="value",
    )
    .filter(F.col("value").isNotNull())
)
