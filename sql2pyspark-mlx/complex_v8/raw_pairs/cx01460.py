from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_is_explicit_by_release_date = Window.partitionBy("is_explicit").orderBy(F.col("release_date").desc(), "id")
w_all_by_duration_sec_filled_t = Window.orderBy(F.col("duration_sec_filled").desc())
w_is_explicit_by_release_date_2 = Window.partitionBy("is_explicit").orderBy(F.col("release_date").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_is_explicit_by_release_date_3 = Window.partitionBy("is_explicit").orderBy(F.col("release_date").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_artist_id_is_explicit_label = Window.partitionBy("artist_id", "is_explicit_label")

valid_tracks = (
    df_tracks
    .filter(F.col("duration_sec").between(144, 344))
    .select(
        "id",
        "artist_id",
        "title",
        "duration_sec",
        "release_date",
        "is_explicit",
        "bpm",
        "moods",
        F.coalesce(F.col("duration_sec"), F.lit(0)).alias("duration_sec_filled"),
        F.when(F.col("is_explicit"), "yes").otherwise("no").alias("is_explicit_label"),
        F.lower(F.trim(F.col("title"))).alias("title_clean"),
    )
)

tagged = (
    valid_tracks
    .filter(~F.col("is_explicit"))
    .select(
        "*",
        F.when(F.col("is_explicit"), 1).otherwise(0).alias("is_explicit_flag"),
        F.round(F.col("bpm") / F.nullif(F.col("duration_sec"), F.lit(0)), 3).alias("bpm_per_duration_sec"),
    )
)

windowed = (
    tagged
    .select(
        "*",
        (F.col("duration_sec_filled") - F.lag("duration_sec_filled").over(w_is_explicit_by_release_date)).alias("duration_sec_filled_change"),
        F.rank().over(w_all_by_duration_sec_filled_t).alias("duration_sec_filled_rank"),
        F.nth_value("duration_sec", 2).over(w_is_explicit_by_release_date_2).alias("second_duration_sec"),
        F.min("bpm_per_duration_sec").over(w_is_explicit_by_release_date_3).alias("min_bpm_per_duration_sec_so_far"),
        F.ntile(4).over(w_is_explicit_by_release_date).alias("release_date_quartile"),
        F.sum("bpm_per_duration_sec").over(w_artist_id_is_explicit_label).alias("artist_id_is_explicit_label_sum_bpm_per_duration"),
    )
)

joined = (
    windowed.alias("s")
    .join(df_artists.alias("a"), F.col("a.id") == F.col("s.artist_id"), "inner")
    .select(
        "s.*",
        F.col("a.debut_year").alias("artist_debut_year"),
        F.col("a.genre").alias("artist_genre"),
    )
)

track_stats = (
    df_tracks
    .groupBy("artist_id")
    .agg(F.min("release_date").alias("first_track_release_date"))
)

summarized_rows = (
    joined.alias("s")
    .join(track_stats.alias("t"), F.col("t.artist_id") == F.col("s.artist_id"), "left")
    .select("s.*", "t.first_track_release_date")
)

exploded_detail = (
    summarized_rows.alias("s")
    .join(df_plays.alias("p"), (F.col("p.track_id") == F.col("s.id")) & (F.col("p.listener_id").isNotNull()), "left")
    .select(
        "s.id",
        "s.artist_id",
        "s.title",
        "s.release_date",
        "s.bpm",
        "s.moods",
        "s.is_explicit_label",
        "s.title_clean",
        "s.is_explicit_flag",
        "s.bpm_per_duration_sec",
        "s.duration_sec_filled_change",
        "s.duration_sec_filled_rank",
        "s.second_duration_sec",
        "s.artist_id_is_explicit_label_sum_bpm_per_duration",
        "s.artist_debut_year",
        "s.artist_genre",
        F.col("p.id").alias("play_id"),
        F.col("p.is_skipped").alias("play_is_skipped"),
        F.col("p.device").alias("play_device"),
        F.col("p.listener_id").alias("play_listener_id"),
    )
)

result = (
    exploded_detail
    .select(
        "id",
        "play_id",
        F.col("artist_id_is_explicit_label_sum_bpm_per_duration").alias("m_artist_id_is_explicit_label_sum_bpm_per_durati"),
        F.col("bpm_per_duration_sec").alias("bpm_per_duration_sec_v"),
    )
    .unpivot(
        ids=["id", "play_id"],
        values=["m_artist_id_is_explicit_label_sum_bpm_per_durati", "bpm_per_duration_sec_v"],
        variableColumnName="metric",
        valueColumnName="amount",
    )
    .filter(F.col("amount").isNotNull())
    .orderBy("id", "play_id", "metric")
)
