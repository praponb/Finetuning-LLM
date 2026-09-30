from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_is_explicit_artist_is_verifi = Window.partitionBy("is_explicit", "artist_is_verified")
w_artist_is_verified_artist_id = Window.partitionBy("artist_is_verified", "artist_id").orderBy("duration_sec", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_artist_id_by_duration_sec = Window.partitionBy("artist_id").orderBy(F.col("duration_sec").desc(), "id")
w_artist_id_artist_is_verified = Window.partitionBy("artist_id", "artist_is_verified").orderBy("title_len", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_artist_id_artist_is_verified_2 = Window.partitionBy("artist_id", "artist_is_verified").orderBy("title_len", "id")
w_artist_id_by_duration_sec_fu = Window.partitionBy("artist_id").orderBy(F.col("duration_sec").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_artist_is_verified_artist_id_2 = Window.partitionBy("artist_is_verified", "artist_id").orderBy("duration_sec", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_second_title_len_2 = Window.partitionBy("second_title_len_2")
w_all_by_is_explicit_artist_is = Window.orderBy(F.col("is_explicit_artist_is_verified_avg_bpm_per_durat").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

filtered = (
    df_tracks
    .filter(F.col("bpm") > 85)
    .select(
        "id",
        "artist_id",
        "duration_sec",
        "is_explicit",
        "bpm",
        "moods",
        F.length(F.trim(F.col("title"))).alias("title_len"),
        F.round(F.col("bpm") / F.nullif(F.col("duration_sec"), F.lit(0)), 3).alias("bpm_per_duration_sec"),
    )
)

with_details_artist = (
    filtered.alias("s")
    .join(df_artists.alias("a"), F.col("a.id") == F.col("s.artist_id"), "inner")
    .select("s.*", F.col("a.is_verified").alias("artist_is_verified"))
)

with_windows = (
    with_details_artist
    .select(
        "id",
        "artist_id",
        "duration_sec",
        "is_explicit",
        "title_len",
        "artist_is_verified",
        F.round(F.col("bpm_per_duration_sec") - F.avg("bpm_per_duration_sec").over(w_is_explicit_artist_is_verifi), 2).alias("bpm_per_duration_sec_vs_is_explicit_artist_is_ve"),
        F.round(F.col("duration_sec") - F.avg("duration_sec").over(w_is_explicit_artist_is_verifi), 2).alias("duration_sec_vs_is_explicit_artist_is_verified_a"),
        F.first("bpm").over(w_artist_is_verified_artist_id).alias("first_bpm"),
        (F.col("duration_sec") - F.lag("duration_sec").over(w_artist_id_by_duration_sec)).alias("duration_sec_change"),
        F.nth_value("title_len", 2).over(w_artist_id_artist_is_verified).alias("second_title_len"),
        F.round(F.col("title_len") / F.nullif(F.sum("title_len").over(w_is_explicit_artist_is_verifi), F.lit(0)), 4).alias("title_len_share_of_is_explicit_artist_is_verifie"),
        F.min("duration_sec").over(w_is_explicit_artist_is_verifi).alias("is_explicit_artist_is_verified_min_duration_sec"),
        F.count(F.lit(1)).over(w_is_explicit_artist_is_verifi).alias("rows_in_is_explicit_artist_is_verified"),
        F.lead("title_len", 2, 0).over(w_artist_id_artist_is_verified_2).alias("next2_title_len"),
        F.nth_value("title_len", 2).over(w_artist_id_by_duration_sec_fu).alias("second_title_len_2"),
        F.round(F.avg("bpm_per_duration_sec").over(w_is_explicit_artist_is_verifi), 2).alias("is_explicit_artist_is_verified_avg_bpm_per_durat"),
        F.sum("bpm_per_duration_sec").over(w_artist_is_verified_artist_id_2).alias("running_bpm_per_duration_sec"),
    )
)

ranked = (
    with_windows
    .select(
        "id",
        "artist_id",
        "duration_sec",
        "is_explicit",
        "bpm_per_duration_sec_vs_is_explicit_artist_is_ve",
        "first_bpm",
        "duration_sec_change",
        "second_title_len",
        "title_len_share_of_is_explicit_artist_is_verifie",
        "is_explicit_artist_is_verified_min_duration_sec",
        "rows_in_is_explicit_artist_is_verified",
        "next2_title_len",
        "second_title_len_2",
        "running_bpm_per_duration_sec",
        F.count(F.lit(1)).over(w_second_title_len_2).alias("rows_in_second_title_len_2"),
        F.round(F.col("duration_sec") / F.nullif(F.sum("duration_sec").over(w_second_title_len_2), F.lit(0)), 4).alias("duration_sec_share_of_second_title_len_2"),
        F.first("title_len").over(w_all_by_is_explicit_artist_is).alias("first_title_len"),
        F.size(F.collect_set("artist_id").over(w_second_title_len_2)).alias("distinct_artist_id_in_second_title_len_2"),
    )
)

result = (
    ranked
    .filter(F.col("artist_id").isNotNull())
    .select(
        "id",
        "artist_id",
        "duration_sec",
        "bpm_per_duration_sec_vs_is_explicit_artist_is_ve",
        "first_bpm",
        "duration_sec_change",
        "second_title_len",
        "title_len_share_of_is_explicit_artist_is_verifie",
        "rows_in_is_explicit_artist_is_verified",
        "second_title_len_2",
        "running_bpm_per_duration_sec",
        "duration_sec_share_of_second_title_len_2",
        "first_title_len",
        "distinct_artist_id_in_second_title_len_2",
        F.greatest(F.col("duration_sec_change"), F.col("next2_title_len")).alias("greatest_duration_sec_change_next2_title_len"),
    )
    .orderBy(F.col("duration_sec").asc_nulls_last())
)
