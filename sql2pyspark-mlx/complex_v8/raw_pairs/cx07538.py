from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_duration_sec_ties = Window.orderBy("duration_sec")
w_is_explicit_artist_id_by_dur = Window.partitionBy("is_explicit", "artist_id").orderBy(F.col("duration_sec").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_artist_id_by_duration_sec_ti = Window.partitionBy("artist_id").orderBy(F.col("duration_sec").desc())
w_artist_country = Window.partitionBy("artist_country")
w_artist_genre = Window.partitionBy("artist_genre")
w_all_by_duration_sec = Window.orderBy("duration_sec", "id")
w_artist_id_by_duration_sec = Window.partitionBy("artist_id").orderBy(F.col("duration_sec").desc(), "id")

base = (
    df_tracks
    .select(
        "id",
        "artist_id",
        "title",
        "duration_sec",
        "release_date",
        "is_explicit",
        F.initcap(F.trim(F.col("title"))).alias("title_clean"),
    )
)

enriched_artist = (
    base.alias("s")
    .join(df_artists.alias("a"), F.col("a.id") == F.col("s.artist_id"), "left")
    .select(
        "s.id",
        "s.artist_id",
        "s.title",
        "s.duration_sec",
        "s.is_explicit",
        "s.title_clean",
        F.col("a.country").alias("artist_country"),
        F.col("a.genre").alias("artist_genre"),
    )
)

with_windows = (
    enriched_artist
    .select(
        "id",
        "title",
        "duration_sec",
        "is_explicit",
        "title_clean",
        F.round(F.cume_dist().over(w_all_by_duration_sec_ties), 3).alias("duration_sec_cume_dist"),
        F.nth_value("duration_sec", 2).over(w_is_explicit_artist_id_by_dur).alias("second_duration_sec"),
        F.round(F.cume_dist().over(w_artist_id_by_duration_sec_ti), 3).alias("duration_sec_cume_dist_2"),
        F.round(F.col("duration_sec") / F.nullif(F.sum("duration_sec").over(w_artist_country), F.lit(0)), 4).alias("duration_sec_share_of_artist_country"),
        F.round(F.col("duration_sec") / F.nullif(F.sum("duration_sec").over(w_artist_genre), F.lit(0)), 4).alias("duration_sec_share_of_artist_genre"),
        F.lag("duration_sec", 3, 0).over(w_all_by_duration_sec).alias("prev3_duration_sec"),
        (F.col("duration_sec") - F.lag("duration_sec").over(w_artist_id_by_duration_sec)).alias("duration_sec_change"),
        F.sum("duration_sec").over(w_artist_country).alias("artist_country_sum_duration_sec"),
    )
)

result = (
    with_windows
    .filter(F.col("duration_sec_share_of_artist_genre").between(0.3, 0.8))
    .select(
        "id",
        "title",
        "is_explicit",
        "title_clean",
        "second_duration_sec",
        "duration_sec_cume_dist_2",
        "duration_sec_share_of_artist_genre",
        "duration_sec_change",
        "artist_country_sum_duration_sec",
        F.initcap(F.trim(F.col("title_clean"))).alias("title_clean_clean"),
        F.lower(F.regexp_replace(F.trim(F.col("title")), " ", "_")).alias("title_slug"),
    )
    .orderBy(
        F.col("duration_sec_cume_dist_2").desc_nulls_first(),
        "title_clean_clean",
    )
)
