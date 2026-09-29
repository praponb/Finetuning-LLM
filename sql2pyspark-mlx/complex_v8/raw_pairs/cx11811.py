from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_release_date_month_by_bpm_mo = Window.partitionBy(F.month("release_date")).orderBy(F.col("bpm").desc_nulls_first(), "id").rowsBetween(-3, Window.currentRow)
w_is_explicit_by_bpm = Window.partitionBy("is_explicit").orderBy(F.col("bpm").desc(), "id")
w_is_explicit_by_bpm_running = Window.partitionBy("is_explicit").orderBy(F.col("bpm").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_release_date_month_by_bpm = Window.partitionBy(F.month("release_date")).orderBy(F.col("bpm").desc_nulls_first(), "id")
w_artist_country = Window.partitionBy("artist_country")

base_tracks = (
    df_tracks
    .filter(F.col("duration_sec").between(184, 409))
    .select(
        "id",
        "artist_id",
        "duration_sec",
        "release_date",
        "is_explicit",
        "bpm",
        "moods",
    )
)

with_details_artist = (
    base_tracks.alias("s")
    .join(df_artists.alias("a"), F.col("a.id") == F.col("s.artist_id"), "left")
    .select(
        "s.id",
        "s.artist_id",
        "s.release_date",
        "s.is_explicit",
        "s.bpm",
        "s.moods",
        F.col("a.country").alias("artist_country"),
    )
)

metrics = (
    with_details_artist
    .select(
        "id",
        "is_explicit",
        "bpm",
        "moods",
        "artist_country",
        F.sum("bpm").over(w_release_date_month_by_bpm_mo).alias("bpm_sum_4"),
        F.datediff(F.col("release_date"), F.lag("release_date").over(w_is_explicit_by_bpm)).alias("days_since_prev_release_date"),
        F.row_number().over(w_is_explicit_by_bpm).alias("rn"),
        F.ntile(10).over(w_is_explicit_by_bpm).alias("bpm_decile"),
        F.max("bpm").over(w_is_explicit_by_bpm_running).alias("max_bpm_so_far"),
        F.row_number().over(w_release_date_month_by_bpm).alias("bpm_seq"),
        F.max("bpm").over(w_artist_country).alias("artist_country_max_bpm"),
        F.min("bpm").over(w_artist_country).alias("artist_country_min_bpm"),
        F.round(F.avg("bpm").over(w_artist_country), 2).alias("artist_country_avg_bpm"),
    )
)

top_per_group = (
    metrics
    .filter(F.col("rn") == 1)
    .drop("rn")
)

result = (
    top_per_group
    .filter(F.size(F.col("moods")) >= 1)
    .select(
        "id",
        "is_explicit",
        "moods",
        "artist_country",
        "bpm_sum_4",
        "days_since_prev_release_date",
        "bpm_decile",
        "max_bpm_so_far",
        "artist_country_max_bpm",
        "artist_country_min_bpm",
        "artist_country_avg_bpm",
        F.when(F.col("bpm_sum_4") >= 468, "A").when(F.col("bpm_sum_4") >= 204, "B").otherwise("C").alias("bpm_sum_4_tier"),
        F.greatest(F.col("bpm_decile"), F.col("artist_country_min_bpm")).alias("greatest_bpm_decile_artist_country_min_bpm"),
    )
    .orderBy(F.col("bpm_sum_4").desc_nulls_first(), "id")
    .limit(5)
)
