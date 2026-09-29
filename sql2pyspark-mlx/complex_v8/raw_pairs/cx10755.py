from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_artist_country_artist_is_ver = Window.partitionBy("artist_country", "artist_is_verified").orderBy("duration_sec", "id").rowsBetween(-2, Window.currentRow)
w_artist_country_artist_is_ver_2 = Window.partitionBy("artist_country", "artist_is_verified").orderBy("duration_sec", "id")
w_artist_is_verified = Window.partitionBy("artist_is_verified")
w_all_by_duration_sec_running = Window.orderBy("duration_sec", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

cleaned_tracks = (
    df_tracks
    .filter((F.col("artist_id") >= 8) & (F.upper(F.trim(F.col("title"))).like("S%")))
    .select(
        "id",
        "artist_id",
        "title",
        "duration_sec",
        "release_date",
        "is_explicit",
        "bpm",
        F.concat(F.col("title"), F.lit("#"), F.col("artist_id").cast("string")).alias("title_ref"),
        F.lower(F.trim(F.col("title"))).alias("title_clean"),
    )
)

enriched = (
    cleaned_tracks.alias("s")
    .join(df_artists.alias("a"), F.col("a.id") == F.col("s.artist_id"), "left")
    .select(
        "s.id",
        "s.artist_id",
        "s.duration_sec",
        "s.release_date",
        "s.bpm",
        F.col("a.is_verified").alias("artist_is_verified"),
        F.col("a.country").alias("artist_country"),
    )
)

sequenced = (
    enriched
    .select(
        "*",
        F.round(F.avg("duration_sec").over(w_artist_country_artist_is_ver), 2).alias("duration_sec_avg_3"),
        F.round(F.percent_rank().over(w_artist_country_artist_is_ver_2), 3).alias("duration_sec_percent_rank"),
        F.size(F.collect_set("artist_country").over(w_artist_is_verified)).alias("distinct_artist_country_in_artist_is_verified"),
        F.sum("duration_sec").over(w_artist_is_verified).alias("artist_is_verified_sum_duration_sec"),
        F.round(F.col("duration_sec") - F.avg("duration_sec").over(w_artist_is_verified), 2).alias("duration_sec_vs_artist_is_verified_avg"),
        F.first("bpm", ignorenulls=True).over(w_all_by_duration_sec_running).alias("first_known_bpm"),
        F.size(F.collect_set("artist_id").over(w_artist_is_verified)).alias("distinct_artist_id_in_artist_is_verified"),
        F.round(F.col("bpm") / F.nullif(F.sum("bpm").over(w_artist_is_verified), F.lit(0)), 4).alias("bpm_share_of_artist_is_verified"),
        F.count(F.lit(1)).over(w_artist_is_verified).alias("rows_in_artist_is_verified"),
    )
)

result = (
    sequenced
    .select(
        "id",
        "release_date",
        "bpm",
        "artist_country",
        "duration_sec_avg_3",
        "distinct_artist_country_in_artist_is_verified",
        "duration_sec_vs_artist_is_verified_avg",
        "first_known_bpm",
        "bpm_share_of_artist_is_verified",
        "rows_in_artist_is_verified",
        F.substring(F.trim(F.col("artist_country")), 1, 2).alias("artist_country_prefix"),
        F.month(F.col("release_date")).alias("release_date_month"),
    )
    .orderBy(
        F.col("duration_sec_vs_artist_is_verified_avg").desc(),
        F.col("duration_sec_avg_3").asc(),
    )
)
