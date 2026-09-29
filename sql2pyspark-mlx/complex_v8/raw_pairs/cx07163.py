from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_artist_country = Window.partitionBy("artist_country")
w_all_by_played_at_full = Window.orderBy("played_at", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_is_skipped_by_artist_debut_y = Window.partitionBy("is_skipped").orderBy("artist_debut_year", "id")
w_all_by_artist_debut_year_mov = Window.orderBy(F.col("artist_debut_year").desc(), "id").rowsBetween(-1, Window.currentRow)
w_all_by_artist_debut_year = Window.orderBy(F.col("artist_debut_year").desc(), "id")
w_played_at_month_track_is_exp = Window.partitionBy(F.month("played_at"), "track_is_explicit").orderBy(F.col("artist_debut_year").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_artist_debut_year_ful = Window.orderBy(F.col("artist_debut_year").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_is_skipped_by_artist_debut_y_2 = Window.partitionBy("is_skipped").orderBy("artist_debut_year", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_artist_debut_year_decile_tra = Window.partitionBy("artist_debut_year_decile", "track_is_explicit")
w_is_skipped_source_by_artist = Window.partitionBy("is_skipped", "source").orderBy("artist_debut_year_rank", "id").rowsBetween(-3, -1)
w_all_by_ms_played_vs_artist_c = Window.orderBy("ms_played_vs_artist_country_avg", "id")
w_is_skipped_source_by_artist_2 = Window.partitionBy("is_skipped", "source").orderBy("artist_debut_year_rank", "id")
w_is_skipped_source_by_artist_3 = Window.partitionBy("is_skipped", "source").orderBy("artist_debut_year_rank", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

valid = (
    df_plays
    .filter((F.col("listener_id").isNotNull()) | (F.col("source") != "search"))
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "ms_played",
        "device",
        "is_skipped",
        "source",
        F.concat_ws(": ", F.col("device"), F.col("source")).alias("device_source_label"),
    )
)

with_parent = (
    valid.alias("s")
    .join(df_tracks.alias("t"), F.col("t.id") == F.col("s.track_id"), "inner")
    .join(df_artists.alias("a"), F.col("a.id") == F.col("t.artist_id"), "inner")
    .select(
        "s.id",
        "s.listener_id",
        "s.track_id",
        "s.played_at",
        "s.ms_played",
        "s.device",
        "s.is_skipped",
        "s.source",
        "s.device_source_label",
        F.col("t.is_explicit").alias("track_is_explicit"),
        F.col("t.title").alias("track_title"),
        F.col("a.debut_year").alias("artist_debut_year"),
        F.col("a.country").alias("artist_country"),
    )
)

sequenced = (
    with_parent
    .select(
        "*",
        F.max("artist_debut_year").over(w_artist_country).alias("artist_country_max_artist_debut_year"),
        F.nth_value("artist_debut_year", 2).over(w_all_by_played_at_full).alias("second_artist_debut_year"),
        F.round(F.col("ms_played") - F.avg("ms_played").over(w_artist_country), 2).alias("ms_played_vs_artist_country_avg"),
        F.ntile(10).over(w_is_skipped_by_artist_debut_y).alias("artist_debut_year_decile"),
        F.max("ms_played").over(w_all_by_artist_debut_year_mov).alias("ms_played_max_2"),
        F.rank().over(w_all_by_artist_debut_year).alias("artist_debut_year_rank"),
        F.nth_value("artist_debut_year", 2).over(w_played_at_month_track_is_exp).alias("second_artist_debut_year_2"),
        F.max("ms_played").over(w_artist_country).alias("artist_country_max_ms_played"),
        F.last("artist_debut_year").over(w_all_by_artist_debut_year_ful).alias("last_artist_debut_year"),
        F.lag("played_at").over(w_is_skipped_by_artist_debut_y).alias("prev_played_at"),
        F.round(F.avg("ms_played").over(w_artist_country), 2).alias("artist_country_avg_ms_played"),
        F.size(F.collect_set("source").over(w_artist_country)).alias("distinct_source_in_artist_country"),
        F.first("artist_debut_year").over(w_is_skipped_by_artist_debut_y_2).alias("first_artist_debut_year"),
    )
)

windowed = (
    sequenced
    .select(
        "id",
        "listener_id",
        "track_id",
        "is_skipped",
        "source",
        "device_source_label",
        "track_is_explicit",
        "track_title",
        "artist_debut_year",
        "artist_country",
        "artist_country_max_artist_debut_year",
        "artist_debut_year_decile",
        "ms_played_max_2",
        "artist_country_max_ms_played",
        "artist_country_avg_ms_played",
        "distinct_source_in_artist_country",
        "first_artist_debut_year",
        F.min("ms_played_max_2").over(w_artist_debut_year_decile_tra).alias("artist_debut_year_decile_track_is_explicit_min_m"),
        F.round(F.col("second_artist_debut_year_2") - F.avg("second_artist_debut_year_2").over(w_artist_debut_year_decile_tra), 2).alias("second_artist_debut_year_2_vs_artist_debut_year_"),
        F.max("artist_country_max_ms_played").over(w_artist_debut_year_decile_tra).alias("artist_debut_year_decile_track_is_explicit_max_a"),
        F.max("artist_debut_year_rank").over(w_is_skipped_source_by_artist).alias("artist_debut_year_rank_max_3_prior"),
        F.lead("artist_debut_year_decile").over(w_all_by_ms_played_vs_artist_c).alias("next_artist_debut_year_decile"),
        F.lead("artist_country_avg_ms_played", 3).over(w_is_skipped_source_by_artist_2).alias("next3_artist_country_avg_ms_played"),
        F.count(F.lit(1)).over(w_artist_debut_year_decile_tra).alias("rows_in_artist_debut_year_decile_track_is_explic"),
        F.nth_value("ms_played_max_2", 2).over(w_is_skipped_source_by_artist_3).alias("second_ms_played_max_2"),
    )
)

result = (
    windowed
    .select(
        "id",
        "listener_id",
        "track_id",
        "is_skipped",
        "source",
        "device_source_label",
        "artist_country",
        "artist_debut_year_decile",
        "ms_played_max_2",
        "artist_country_max_ms_played",
        "artist_country_avg_ms_played",
        "distinct_source_in_artist_country",
        "first_artist_debut_year",
        "artist_debut_year_decile_track_is_explicit_max_a",
        "next_artist_debut_year_decile",
        "next3_artist_country_avg_ms_played",
        "second_ms_played_max_2",
    )
)
