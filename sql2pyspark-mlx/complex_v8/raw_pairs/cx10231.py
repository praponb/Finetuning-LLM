from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_release_date_month = Window.partitionBy(F.month("release_date"))
w_artist_id_by_release_date_ti = Window.partitionBy("artist_id").orderBy(F.col("release_date").desc())
w_artist_id_by_release_date_mo = Window.partitionBy("artist_id").orderBy(F.col("release_date").desc(), "id").rowsBetween(-3, Window.currentRow)
w_artist_id_by_release_date = Window.partitionBy("artist_id").orderBy(F.col("release_date").desc(), "id")
w_release_date_month_by_bpm = Window.partitionBy(F.month("release_date")).orderBy(F.col("bpm").desc(), "id")
w_artist_id_artist_country_by = Window.partitionBy("artist_id", "artist_country").orderBy(F.col("release_date").desc(), "id")

cleaned_tracks = (
    df_tracks
    .filter((F.col("bpm").between(75, 163)) & (F.upper(F.trim(F.col("title"))).like("C%")))
    .select(
        "id",
        "artist_id",
        "title",
        "duration_sec",
        "release_date",
        "bpm",
        "moods",
        F.coalesce(F.col("bpm"), F.lit(0)).alias("bpm_filled"),
        F.coalesce(F.col("duration_sec"), F.lit(0)).alias("duration_sec_filled"),
    )
)

with_parent = (
    cleaned_tracks.alias("s")
    .join(df_artists.alias("a"), (F.col("a.id") == F.col("s.artist_id")) & (F.length(F.trim(F.col("a.artist_name"))) >= 7), "left")
    .select(
        "s.id",
        "s.artist_id",
        "s.title",
        "s.duration_sec",
        "s.release_date",
        "s.bpm",
        "s.moods",
        "s.bpm_filled",
        F.col("a.country").alias("artist_country"),
        F.col("a.debut_year").alias("artist_debut_year"),
    )
)

windowed = (
    with_parent
    .select(
        "*",
        F.min("bpm").over(w_release_date_month).alias("release_date_month_min_bpm"),
        F.sum("duration_sec").over(w_release_date_month).alias("release_date_month_sum_duration_sec"),
        F.round(F.col("bpm") / F.nullif(F.sum("bpm").over(w_release_date_month), F.lit(0)), 4).alias("bpm_share_of_release_date_month"),
        F.max("duration_sec").over(w_release_date_month).alias("release_date_month_max_duration_sec"),
        F.rank().over(w_artist_id_by_release_date_ti).alias("release_date_rank"),
        F.round(F.avg("bpm").over(w_release_date_month), 2).alias("release_date_month_avg_bpm"),
        F.round(F.avg("bpm").over(w_artist_id_by_release_date_mo), 2).alias("bpm_avg_4"),
        F.count(F.lit(1)).over(w_release_date_month).alias("rows_in_release_date_month"),
        F.lead("artist_debut_year").over(w_artist_id_by_release_date).alias("next_artist_debut_year"),
        F.row_number().over(w_release_date_month_by_bpm).alias("seq_no"),
        (F.col("duration_sec") - F.lag("duration_sec").over(w_artist_id_by_release_date)).alias("duration_sec_change"),
        F.sum("bpm").over(w_release_date_month).alias("release_date_month_sum_bpm"),
        F.ntile(10).over(w_artist_id_artist_country_by).alias("release_date_decile"),
    )
)

enriched = windowed.withColumn("moods_count", F.size(F.col("moods")))

result = (
    enriched
    .select(
        "id",
        "artist_id",
        "title",
        "duration_sec",
        "release_date",
        "moods",
        "bpm_filled",
        "artist_country",
        "artist_debut_year",
        "release_date_month_min_bpm",
        "bpm_share_of_release_date_month",
        "release_date_month_max_duration_sec",
        "bpm_avg_4",
        "rows_in_release_date_month",
        "next_artist_debut_year",
        "duration_sec_change",
        "release_date_month_sum_bpm",
        "release_date_decile",
        F.year(F.col("release_date")).alias("release_date_year"),
        F.when(F.col("moods_count") >= 2, "HIGH").when(F.col("moods_count") >= 1, "MEDIUM").otherwise("LOW").alias("moods_count_tier"),
    )
    .orderBy(
        F.col("release_date_month_sum_bpm").asc_nulls_last(),
        "duration_sec",
        "id",
    )
    .limit(20)
)
