from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_artist_debut_year = Window.orderBy(F.col("artist_debut_year").desc(), "id")
w_all_by_artist_debut_year_mov = Window.orderBy(F.col("artist_debut_year").desc(), "id").rowsBetween(-1, Window.currentRow)
w_artist_id_artist_country = Window.partitionBy("artist_id", "artist_country")
w_artist_id = Window.partitionBy("artist_id")
w_all_by_artist_debut_year_run = Window.orderBy(F.col("artist_debut_year").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

filtered_tracks = (
    df_tracks
    .filter((F.col("bpm") < 165) & (F.col("release_date") < F.lit("2025-08-02").cast("date")))
    .select(
        "id",
        "artist_id",
        "title",
        "is_explicit",
        "bpm",
        F.when(F.col("bpm") >= 155, "A").when(F.col("bpm") >= 104, "B").otherwise("C").alias("bpm_tier"),
    )
)

combined_details = (
    filtered_tracks.alias("s")
    .join(df_artists.alias("a"), F.col("a.id") == F.col("s.artist_id"), "inner")
    .select(
        "s.id",
        "s.artist_id",
        "s.title",
        "s.is_explicit",
        F.col("a.debut_year").alias("artist_debut_year"),
        F.col("a.country").alias("artist_country"),
    )
)

with_trends = (
    combined_details
    .select(
        "*",
        (F.col("artist_debut_year") - F.lag("artist_debut_year").over(w_all_by_artist_debut_year)).alias("artist_debut_year_change"),
        F.lead("artist_debut_year", 2).over(w_all_by_artist_debut_year).alias("next2_artist_debut_year"),
        F.round(F.avg("artist_debut_year").over(w_all_by_artist_debut_year_mov), 2).alias("artist_debut_year_avg_2"),
        F.max("artist_debut_year").over(w_artist_id_artist_country).alias("artist_id_artist_country_max_artist_debut_year"),
        F.dense_rank().over(w_all_by_artist_debut_year).alias("artist_debut_year_dense_rank"),
        F.count(F.lit(1)).over(w_artist_id).alias("rows_in_artist_id"),
        F.round(F.col("artist_debut_year") - F.avg("artist_debut_year").over(w_artist_id), 2).alias("artist_debut_year_vs_artist_id_avg"),
        F.max("artist_debut_year").over(w_artist_id).alias("artist_id_max_artist_debut_year"),
        F.sum("artist_debut_year").over(w_artist_id).alias("artist_id_sum_artist_debut_year"),
        F.round(F.avg("artist_debut_year").over(w_artist_id_artist_country), 2).alias("artist_id_artist_country_avg_artist_debut_year"),
        F.count(F.lit(1)).over(w_all_by_artist_debut_year_run).alias("running_count"),
        F.round(F.avg("artist_debut_year").over(w_artist_id), 2).alias("artist_id_avg_artist_debut_year"),
        F.round(F.col("artist_debut_year") / F.nullif(F.sum("artist_debut_year").over(w_artist_id_artist_country), F.lit(0)), 4).alias("artist_debut_year_share_of_artist_id_artist_coun"),
    )
)

result = (
    with_trends
    .select(
        "id",
        "artist_id",
        "title",
        "is_explicit",
        "artist_debut_year",
        "artist_country",
        "artist_debut_year_change",
        "next2_artist_debut_year",
        "artist_debut_year_avg_2",
        "artist_debut_year_dense_rank",
        "rows_in_artist_id",
        "artist_debut_year_vs_artist_id_avg",
        "artist_id_max_artist_debut_year",
        "running_count",
        "artist_id_avg_artist_debut_year",
        "artist_debut_year_share_of_artist_id_artist_coun",
    )
    .orderBy(F.col("title").desc(), F.col("running_count").desc())
)
