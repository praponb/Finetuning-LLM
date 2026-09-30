from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_artist_genre_artist_id_by_gr = Window.partitionBy("artist_genre", "artist_id").orderBy(F.col("greatest_duration_sec_bpm").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_artist_genre_artist_id_by_gr_2 = Window.partitionBy("artist_genre", "artist_id").orderBy(F.col("greatest_duration_sec_bpm").desc(), "id")
w_artist_genre_by_greatest_dur = Window.partitionBy("artist_genre").orderBy(F.col("greatest_duration_sec_bpm").asc_nulls_last())
w_artist_id_artist_country = Window.partitionBy("artist_id", "artist_country")
w_artist_id_by_duration_sec = Window.partitionBy("artist_id").orderBy(F.col("duration_sec").desc(), "id")
w_artist_genre_artist_id_by_gr_3 = Window.partitionBy("artist_genre", "artist_id").orderBy(F.col("greatest_duration_sec_bpm").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_artist_country_is_explicit = Window.partitionBy("artist_country", "is_explicit")
w_artist_genre_artist_is_verif = Window.partitionBy("artist_genre", "artist_is_verified").orderBy(F.col("duration_sec").desc())
w_artist_country = Window.partitionBy("artist_country")
w_artist_genre_artist_is_verif_2 = Window.partitionBy("artist_genre", "artist_is_verified").orderBy(F.col("distinct_artist_genre_in_artist_country_is_expli").desc(), "id")
w_artist_is_verified_artist_co = Window.partitionBy("artist_is_verified", "artist_country").orderBy("prev_greatest_duration_sec_bpm", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_distinct_artist_genre_in_art = Window.partitionBy("distinct_artist_genre_in_artist_country_is_expli").orderBy("artist_country_is_explicit_min_duration_sec", "id")

recent = (
    df_tracks
    .filter(F.col("duration_sec").between(144, 352))
    .select(
        "id",
        "artist_id",
        "duration_sec",
        "is_explicit",
        "moods",
        F.length(F.trim(F.col("title"))).alias("title_len"),
        F.greatest(F.col("duration_sec"), F.col("bpm")).alias("greatest_duration_sec_bpm"),
    )
)

joined_artist = (
    recent.alias("s")
    .join(df_artists.alias("a"), F.col("a.id") == F.col("s.artist_id"), "left")
    .select(
        "s.id",
        "s.artist_id",
        "s.duration_sec",
        "s.is_explicit",
        "s.moods",
        "s.greatest_duration_sec_bpm",
        F.col("a.genre").alias("artist_genre"),
        F.col("a.country").alias("artist_country"),
        F.col("a.is_verified").alias("artist_is_verified"),
    )
)

with_trends = (
    joined_artist
    .select(
        "*",
        F.last("greatest_duration_sec_bpm", ignorenulls=True).over(w_artist_genre_artist_id_by_gr).alias("last_known_greatest_duration_sec_bpm"),
        F.ntile(5).over(w_artist_genre_artist_id_by_gr_2).alias("greatest_duration_sec_bpm_quintile"),
        F.round(F.cume_dist().over(w_artist_genre_by_greatest_dur), 3).alias("greatest_duration_sec_bpm_cume_dist"),
        F.round(F.col("duration_sec") - F.avg("duration_sec").over(w_artist_id_artist_country), 2).alias("duration_sec_vs_artist_id_artist_country_avg"),
        F.size(F.collect_set("artist_genre").over(w_artist_id_artist_country)).alias("distinct_artist_genre_in_artist_id_artist_countr"),
        F.lag("greatest_duration_sec_bpm").over(w_artist_id_by_duration_sec).alias("prev_greatest_duration_sec_bpm"),
        F.first("greatest_duration_sec_bpm").over(w_artist_genre_artist_id_by_gr_3).alias("first_greatest_duration_sec_bpm"),
        F.min("duration_sec").over(w_artist_country_is_explicit).alias("artist_country_is_explicit_min_duration_sec"),
        F.rank().over(w_artist_genre_artist_is_verif).alias("duration_sec_rank"),
        F.round(F.col("greatest_duration_sec_bpm") / F.nullif(F.sum("greatest_duration_sec_bpm").over(w_artist_country_is_explicit), F.lit(0)), 4).alias("greatest_duration_sec_bpm_share_of_artist_countr"),
        F.size(F.collect_set("artist_genre").over(w_artist_country_is_explicit)).alias("distinct_artist_genre_in_artist_country_is_expli"),
    )
)

windowed = (
    with_trends
    .select(
        "*",
        F.round(F.col("greatest_duration_sec_bpm") / F.nullif(F.sum("greatest_duration_sec_bpm").over(w_artist_country), F.lit(0)), 4).alias("greatest_duration_sec_bpm_share_of_artist_countr_2"),
        F.min("greatest_duration_sec_bpm").over(w_artist_country).alias("artist_country_min_greatest_duration_sec_bpm"),
        F.lead("greatest_duration_sec_bpm").over(w_artist_genre_artist_is_verif_2).alias("next_greatest_duration_sec_bpm"),
        F.nth_value("duration_sec_rank", 2).over(w_artist_is_verified_artist_co).alias("second_duration_sec_rank"),
        F.rank().over(w_distinct_artist_genre_in_art).alias("artist_country_is_explicit_min_duration_sec_rank"),
    )
)

result = (
    windowed
    .filter(F.col("duration_sec_vs_artist_id_artist_country_avg").between(-252.0, 378.0))
    .select(
        "id",
        "artist_id",
        "duration_sec",
        "is_explicit",
        "moods",
        "greatest_duration_sec_bpm",
        "artist_genre",
        "artist_country",
        "artist_is_verified",
        "last_known_greatest_duration_sec_bpm",
        "greatest_duration_sec_bpm_quintile",
        "distinct_artist_genre_in_artist_id_artist_countr",
        "prev_greatest_duration_sec_bpm",
        "artist_country_is_explicit_min_duration_sec",
        "duration_sec_rank",
        "greatest_duration_sec_bpm_share_of_artist_countr",
        "distinct_artist_genre_in_artist_country_is_expli",
        "greatest_duration_sec_bpm_share_of_artist_countr_2",
        "artist_country_min_greatest_duration_sec_bpm",
        "next_greatest_duration_sec_bpm",
        "second_duration_sec_rank",
        "artist_country_is_explicit_min_duration_sec_rank",
    )
    .orderBy("artist_id", "distinct_artist_genre_in_artist_id_artist_countr")
)
