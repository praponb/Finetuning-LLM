from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_artist_id_is_explicit_by_dur = Window.partitionBy("artist_id", "is_explicit").orderBy(F.col("duration_sec").desc(), "id")
w_artist_id_artist_is_verified = Window.partitionBy("artist_id", "artist_is_verified")
w_artist_is_verified_by_durati = Window.partitionBy("artist_is_verified").orderBy("duration_sec", "id").rowsBetween(-6, -1)
w_artist_is_verified_by_durati_2 = Window.partitionBy("artist_is_verified").orderBy("duration_sec", "id").rowsBetween(-1, -1)
w_artist_is_verified_by_releas = Window.partitionBy("artist_is_verified").orderBy("release_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_artist_is_verified_by_releas_2 = Window.partitionBy("artist_is_verified").orderBy("release_date", "id")
w_artist_is_verified_by_releas_3 = Window.partitionBy("artist_is_verified").orderBy("release_date", "id").rowsBetween(-2, Window.currentRow)
w_release_date_month_by_durati = Window.partitionBy(F.month("release_date")).orderBy(F.col("duration_sec").desc_nulls_first(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_artist_is_verified_by_durati_3 = Window.partitionBy("artist_is_verified").orderBy("duration_sec", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_is_explicit = Window.partitionBy("is_explicit")
w_artist_is_verified_by_durati_4 = Window.partitionBy("artist_is_verified").orderBy("duration_sec", "id")
w_all_by_artist_debut_year_mov = Window.orderBy("artist_debut_year", "id").rowsBetween(-3, Window.currentRow)

valid_tracks = (
    df_tracks
    .filter(F.col("duration_sec") < 402)
    .select(
        "id",
        "artist_id",
        "title",
        "duration_sec",
        "release_date",
        "is_explicit",
        "moods",
    )
)

combined_details_artist = (
    valid_tracks.alias("s")
    .join(df_artists.alias("a"), (F.col("a.id") == F.col("s.artist_id")) & (F.col("a.genre") == "Classical"), "inner")
    .filter(~F.col("a.is_verified"))
    .select(
        "s.id",
        "s.artist_id",
        "s.title",
        "s.duration_sec",
        "s.release_date",
        "s.is_explicit",
        "s.moods",
        F.col("a.debut_year").alias("artist_debut_year"),
        F.col("a.is_verified").alias("artist_is_verified"),
    )
)

with_windows = (
    combined_details_artist
    .select(
        "id",
        "artist_id",
        "title",
        "release_date",
        "is_explicit",
        "artist_debut_year",
        F.lag("artist_debut_year", 2).over(w_artist_id_is_explicit_by_dur).alias("prev2_artist_debut_year"),
        F.round(F.col("duration_sec") / F.nullif(F.sum("duration_sec").over(w_artist_id_artist_is_verified), F.lit(0)), 4).alias("duration_sec_share_of_artist_id_artist_is_verifi"),
        F.round(F.avg("artist_debut_year").over(w_artist_is_verified_by_durati), 2).alias("artist_debut_year_avg_6_prior"),
        F.max("artist_debut_year").over(w_artist_is_verified_by_durati_2).alias("artist_debut_year_max_1_prior"),
        F.first("artist_debut_year", ignorenulls=True).over(w_artist_is_verified_by_releas).alias("first_known_artist_debut_year"),
        F.lag("artist_debut_year").over(w_artist_is_verified_by_releas_2).alias("prev_artist_debut_year"),
        F.count(F.lit(1)).over(w_artist_id_artist_is_verified).alias("rows_in_artist_id_artist_is_verified"),
        F.max("duration_sec").over(w_artist_is_verified_by_releas_3).alias("duration_sec_max_3"),
        F.nth_value("duration_sec", 2).over(w_release_date_month_by_durati).alias("second_duration_sec"),
        F.max("duration_sec").over(w_artist_is_verified_by_durati_3).alias("max_duration_sec_so_far"),
        F.max("duration_sec").over(w_is_explicit).alias("is_explicit_max_duration_sec"),
        F.last("artist_debut_year", ignorenulls=True).over(w_artist_is_verified_by_releas).alias("last_known_artist_debut_year"),
        F.lead("duration_sec", 2).over(w_artist_is_verified_by_durati_4).alias("next2_duration_sec"),
        F.max("artist_debut_year").over(w_all_by_artist_debut_year_mov).alias("artist_debut_year_max_4"),
        F.round(F.avg("duration_sec").over(w_is_explicit), 2).alias("is_explicit_avg_duration_sec"),
    )
)

scored = (
    with_windows
    .select(
        "id",
        "artist_id",
        "title",
        "release_date",
        "artist_debut_year",
        "prev2_artist_debut_year",
        "duration_sec_share_of_artist_id_artist_is_verifi",
        "artist_debut_year_avg_6_prior",
        "prev_artist_debut_year",
        "duration_sec_max_3",
        "max_duration_sec_so_far",
        "is_explicit_max_duration_sec",
        "last_known_artist_debut_year",
        "artist_debut_year_max_4",
        "is_explicit_avg_duration_sec",
        F.abs(F.col("prev_artist_debut_year") - 2015).alias("prev_artist_debut_year_distance"),
        F.substring(F.trim(F.col("title")), 1, 3).alias("title_prefix"),
        F.lower(F.regexp_replace(F.trim(F.col("title")), " ", "_")).alias("title_slug"),
    )
)

result = (
    scored
    .select(
        "id",
        "artist_id",
        "title",
        "duration_sec_share_of_artist_id_artist_is_verifi",
        "artist_debut_year_avg_6_prior",
        "prev_artist_debut_year",
        "duration_sec_max_3",
        "max_duration_sec_so_far",
        "is_explicit_max_duration_sec",
        "last_known_artist_debut_year",
        "title_prefix",
        "title_slug",
    )
    .orderBy(F.col("last_known_artist_debut_year").desc())
)
