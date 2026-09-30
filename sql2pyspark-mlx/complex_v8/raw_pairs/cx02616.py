from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_artist_is_verified_by_durati = Window.partitionBy("artist_is_verified").orderBy("duration_sec")
w_artist_id_is_explicit = Window.partitionBy("artist_id", "is_explicit")
w_is_explicit_artist_is_verifi = Window.partitionBy("is_explicit", "artist_is_verified").orderBy("artist_debut_year", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_artist_id_artist_is_verified = Window.partitionBy("artist_id", "artist_is_verified")
w_artist_is_verified_release_d = Window.partitionBy("artist_is_verified", F.date_trunc("month", "release_date")).orderBy("bpm", "id")
w_is_explicit_artist_is_verifi_2 = Window.partitionBy("is_explicit", "artist_is_verified").orderBy(F.col("duration_sec").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_artist_is_verified_release_d_2 = Window.partitionBy("artist_is_verified", F.date_trunc("month", "release_date")).orderBy("bpm", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_is_explicit_artist_id_by_dur = Window.partitionBy("is_explicit", "artist_id").orderBy(F.col("duration_sec").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_is_explicit_artist_is_verifi_3 = Window.partitionBy("is_explicit", "artist_is_verified").orderBy("artist_debut_year")
w_artist_id_by_next2_bpm = Window.partitionBy("artist_id").orderBy("next2_bpm", "id")
w_artist_is_verified_first_kno = Window.partitionBy("artist_is_verified", F.month("first_known_release_date"))
w_release_date_month_duration = Window.partitionBy(F.month("release_date"), "duration_sec_rank").orderBy("running_count", "id")
w_bpm_dense_rank = Window.partitionBy("bpm_dense_rank")

valid_tracks = (
    df_tracks
    .filter(F.col("artist_id") >= 6)
    .select(
        "id",
        "artist_id",
        "title",
        "duration_sec",
        "release_date",
        "is_explicit",
        "bpm",
        "moods",
        F.date_format(F.col("release_date"), "yyyy-MM-dd").alias("release_date_label"),
        F.concat(F.col("title"), F.lit("#"), F.col("bpm").cast("string")).alias("title_ref"),
    )
)

with_details_artist = (
    valid_tracks.alias("s")
    .join(df_artists.alias("a"), (F.col("a.id") == F.col("s.artist_id")) & (F.col("a.is_verified")), "left")
    .select(
        "s.id",
        "s.artist_id",
        "s.title",
        "s.duration_sec",
        "s.release_date",
        "s.is_explicit",
        "s.bpm",
        "s.moods",
        "s.title_ref",
        F.col("a.debut_year").alias("artist_debut_year"),
        F.col("a.is_verified").alias("artist_is_verified"),
        "a.artist_name",
    )
)

windowed = (
    with_details_artist
    .select(
        "id",
        "artist_id",
        "release_date",
        "bpm",
        "moods",
        "title_ref",
        "artist_debut_year",
        "artist_is_verified",
        "artist_name",
        F.rank().over(w_artist_is_verified_by_durati).alias("duration_sec_rank"),
        F.max("bpm").over(w_artist_id_is_explicit).alias("artist_id_is_explicit_max_bpm"),
        F.first("release_date", ignorenulls=True).over(w_is_explicit_artist_is_verifi).alias("first_known_release_date"),
        F.count(F.lit(1)).over(w_artist_id_artist_is_verified).alias("rows_in_artist_id_artist_is_verified"),
        F.count(F.lit(1)).over(w_artist_id_is_explicit).alias("rows_in_artist_id_is_explicit"),
        F.dense_rank().over(w_artist_is_verified_release_d).alias("bpm_dense_rank"),
        F.round(F.percent_rank().over(w_artist_is_verified_by_durati), 3).alias("duration_sec_percent_rank"),
        F.last("artist_debut_year", ignorenulls=True).over(w_is_explicit_artist_is_verifi_2).alias("last_known_artist_debut_year"),
        F.lead("duration_sec").over(w_artist_is_verified_release_d).alias("next_duration_sec"),
        F.count(F.lit(1)).over(w_is_explicit_artist_is_verifi_2).alias("running_count"),
        F.lead("bpm", 2).over(w_artist_is_verified_release_d).alias("next2_bpm"),
        F.first("bpm", ignorenulls=True).over(w_artist_is_verified_release_d_2).alias("first_known_bpm"),
        F.first("duration_sec", ignorenulls=True).over(w_is_explicit_artist_id_by_dur).alias("first_known_duration_sec"),
        F.round(F.col("bpm") - F.avg("bpm").over(w_artist_id_artist_is_verified), 2).alias("bpm_vs_artist_id_artist_is_verified_avg"),
        F.rank().over(w_is_explicit_artist_is_verifi_3).alias("artist_debut_year_rank"),
    )
)

first_rows = windowed.filter(F.col("bpm_dense_rank") == 1)

metrics = (
    first_rows
    .select(
        "*",
        (F.col("next2_bpm") - F.lag("next2_bpm").over(w_artist_id_by_next2_bpm)).alias("next2_bpm_change"),
        F.round(F.cume_dist().over(w_artist_id_by_next2_bpm), 3).alias("next2_bpm_cume_dist"),
        F.round(F.col("artist_debut_year_rank") / F.nullif(F.sum("artist_debut_year_rank").over(w_artist_is_verified_first_kno), F.lit(0)), 4).alias("artist_debut_year_rank_share_of_artist_is_verifi"),
        F.row_number().over(w_release_date_month_duration).alias("running_count_seq"),
        F.round(F.percent_rank().over(w_artist_id_by_next2_bpm), 3).alias("next2_bpm_percent_rank"),
        F.round(F.col("artist_debut_year_rank") / F.nullif(F.sum("artist_debut_year_rank").over(w_bpm_dense_rank), F.lit(0)), 4).alias("artist_debut_year_rank_share_of_bpm_dense_rank"),
        F.round(F.avg("duration_sec_percent_rank").over(w_artist_is_verified_first_kno), 2).alias("artist_is_verified_first_known_release_date_mont"),
    )
)

result = (
    metrics
    .select(
        "id",
        "artist_id",
        "release_date",
        "bpm",
        "moods",
        "title_ref",
        "artist_debut_year",
        "artist_is_verified",
        "duration_sec_rank",
        "artist_id_is_explicit_max_bpm",
        "first_known_release_date",
        "rows_in_artist_id_artist_is_verified",
        "bpm_dense_rank",
        "next2_bpm",
        "first_known_bpm",
        "first_known_duration_sec",
        "bpm_vs_artist_id_artist_is_verified_avg",
        "artist_debut_year_rank",
        "next2_bpm_change",
        "artist_debut_year_rank_share_of_artist_is_verifi",
        "next2_bpm_percent_rank",
        "artist_debut_year_rank_share_of_bpm_dense_rank",
        "artist_is_verified_first_known_release_date_mont",
        F.when(F.col("artist_debut_year_rank") >= 7, "large").when(F.col("artist_debut_year_rank") >= 4, "medium").otherwise("small").alias("artist_debut_year_rank_tier"),
    )
    .orderBy("rows_in_artist_id_artist_is_verified")
)
