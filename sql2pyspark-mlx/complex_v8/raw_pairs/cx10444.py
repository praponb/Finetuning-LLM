from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_artist_id_by_duration_sec_mo = Window.partitionBy("artist_id").orderBy("duration_sec", "id").rowsBetween(-2, Window.currentRow)
w_all_by_duration_sec_prior_2 = Window.orderBy(F.col("duration_sec").desc(), "id").rowsBetween(-2, -1)
w_artist_id_artist_is_verified = Window.partitionBy("artist_id", "artist_is_verified")
w_artist_is_verified_by_durati = Window.partitionBy("artist_is_verified").orderBy(F.col("duration_sec").desc(), "id").rowsBetween(-3, Window.currentRow)
w_all_by_duration_sec = Window.orderBy(F.col("duration_sec").desc(), "id")
w_artist_is_verified_by_durati_2 = Window.partitionBy("artist_is_verified").orderBy(F.col("duration_sec").desc(), "id").rowsBetween(-1, Window.currentRow)
w_all_by_duration_sec_full = Window.orderBy(F.col("duration_sec").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_artist_is_verified_by_durati_3 = Window.partitionBy("artist_is_verified").orderBy(F.col("duration_sec").desc())
w_artist_is_verified_by_durati_4 = Window.partitionBy("artist_is_verified").orderBy(F.col("duration_sec").desc(), "id")

prepared = (
    df_tracks
    .filter(F.size(F.col("moods")) >= 1)
    .select(
        "id",
        "artist_id",
        "title",
        "duration_sec",
        "is_explicit",
        "bpm",
        "moods",
        F.date_format(F.col("release_date"), "yyyy-MM").alias("release_date_label"),
    )
)

enriched_artist = (
    prepared.alias("s")
    .join(df_artists.alias("a"), F.col("a.id") == F.col("s.artist_id"), "left")
    .select(
        "s.id",
        "s.artist_id",
        "s.title",
        "s.duration_sec",
        "s.moods",
        "s.release_date_label",
        F.col("a.is_verified").alias("artist_is_verified"),
    )
)

with_trends = (
    enriched_artist
    .select(
        "id",
        "title",
        "moods",
        "release_date_label",
        F.round(F.avg("duration_sec").over(w_artist_id_by_duration_sec_mo), 2).alias("duration_sec_avg_3"),
        F.round(F.avg("duration_sec").over(w_all_by_duration_sec_prior_2), 2).alias("duration_sec_avg_2_prior"),
        F.sum("duration_sec").over(w_artist_id_artist_is_verified).alias("artist_id_artist_is_verified_sum_duration_sec"),
        F.round(F.col("duration_sec") / F.nullif(F.sum("duration_sec").over(w_artist_id_artist_is_verified), F.lit(0)), 4).alias("duration_sec_share_of_artist_id_artist_is_verifi"),
        F.round(F.avg("duration_sec").over(w_artist_is_verified_by_durati), 2).alias("duration_sec_avg_4"),
        F.round(F.cume_dist().over(w_all_by_duration_sec), 3).alias("duration_sec_cume_dist"),
        F.count(F.lit(1)).over(w_artist_id_artist_is_verified).alias("rows_in_artist_id_artist_is_verified"),
        F.max("duration_sec").over(w_artist_is_verified_by_durati_2).alias("duration_sec_max_2"),
        F.first("duration_sec").over(w_all_by_duration_sec_full).alias("first_duration_sec"),
        F.round(F.cume_dist().over(w_artist_is_verified_by_durati_3), 3).alias("duration_sec_cume_dist_2"),
        F.ntile(5).over(w_artist_is_verified_by_durati_4).alias("duration_sec_quintile"),
    )
)

result = (
    with_trends
    .filter(F.array_contains(F.col("moods"), "sad"))
    .select(
        "id",
        "title",
        "release_date_label",
        "duration_sec_avg_2_prior",
        "duration_sec_share_of_artist_id_artist_is_verifi",
        "duration_sec_avg_4",
        "duration_sec_cume_dist",
        "duration_sec_max_2",
        "first_duration_sec",
        "duration_sec_quintile",
        F.initcap(F.trim(F.col("release_date_label"))).alias("release_date_label_clean"),
        F.concat(F.col("release_date_label"), F.lit("#"), F.col("first_duration_sec").cast("string")).alias("release_date_label_ref"),
    )
    .orderBy(
        F.col("duration_sec_avg_4").desc(),
        F.col("duration_sec_cume_dist").desc(),
    )
)
