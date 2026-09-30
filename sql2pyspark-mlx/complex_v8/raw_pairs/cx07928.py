from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_is_explicit_artist_id = Window.partitionBy("is_explicit", "artist_id")
w_all_by_bpm_full = Window.orderBy("bpm", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_artist_debut_year_run = Window.orderBy(F.col("artist_debut_year").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_artist_debut_year_ful = Window.orderBy(F.col("artist_debut_year").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_artist_debut_year = Window.orderBy(F.col("artist_debut_year").desc(), "id")
w_all_by_bpm_moving_6 = Window.orderBy("bpm", "id").rowsBetween(-6, Window.currentRow)
w_is_explicit_artist_debut_yea = Window.partitionBy("is_explicit", "artist_debut_year_rank")
w_artist_id_artist_debut_year = Window.partitionBy("artist_id", "artist_debut_year_rank").orderBy(F.col("is_explicit_artist_id_max_bpm").desc(), "id")
w_artist_id_artist_debut_year_2 = Window.partitionBy("artist_id", "artist_debut_year_rank").orderBy(F.col("is_explicit_artist_id_max_bpm").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_is_explicit = Window.partitionBy("is_explicit")

scoped_tracks = (
    df_tracks
    .filter((F.length(F.trim(F.col("title"))) >= 5) & (~F.array_contains(F.col("moods"), "sad")))
    .select(
        "id",
        "artist_id",
        "title",
        "is_explicit",
        "bpm",
    )
)

joined_artist = (
    scoped_tracks.alias("s")
    .join(df_artists.alias("a"), F.col("a.id") == F.col("s.artist_id"), "left")
    .select(
        "s.id",
        "s.artist_id",
        "s.title",
        "s.is_explicit",
        "s.bpm",
        F.col("a.debut_year").alias("artist_debut_year"),
    )
)

sequenced = (
    joined_artist
    .select(
        "*",
        F.min("artist_debut_year").over(w_is_explicit_artist_id).alias("is_explicit_artist_id_min_artist_debut_year"),
        F.nth_value("bpm", 2).over(w_all_by_bpm_full).alias("second_bpm"),
        F.count(F.lit(1)).over(w_all_by_artist_debut_year_run).alias("running_count"),
        F.sum("bpm").over(w_is_explicit_artist_id).alias("is_explicit_artist_id_sum_bpm"),
        F.first("artist_debut_year").over(w_all_by_artist_debut_year_ful).alias("first_artist_debut_year"),
        F.round(F.avg("artist_debut_year").over(w_is_explicit_artist_id), 2).alias("is_explicit_artist_id_avg_artist_debut_year"),
        F.round(F.col("artist_debut_year") - F.avg("artist_debut_year").over(w_is_explicit_artist_id), 2).alias("artist_debut_year_vs_is_explicit_artist_id_avg"),
        F.count(F.lit(1)).over(w_is_explicit_artist_id).alias("rows_in_is_explicit_artist_id"),
        F.max("bpm").over(w_is_explicit_artist_id).alias("is_explicit_artist_id_max_bpm"),
        F.rank().over(w_all_by_artist_debut_year).alias("artist_debut_year_rank"),
        F.round(F.avg("artist_debut_year").over(w_all_by_bpm_moving_6), 2).alias("artist_debut_year_avg_7"),
        F.round(F.col("bpm") / F.nullif(F.sum("bpm").over(w_is_explicit_artist_id), F.lit(0)), 4).alias("bpm_share_of_is_explicit_artist_id"),
        F.round(F.col("artist_debut_year") / F.nullif(F.sum("artist_debut_year").over(w_is_explicit_artist_id), F.lit(0)), 4).alias("artist_debut_year_share_of_is_explicit_artist_id"),
    )
)

with_trends = (
    sequenced
    .select(
        "*",
        F.round(F.col("artist_debut_year_avg_7") - F.avg("artist_debut_year_avg_7").over(w_is_explicit_artist_debut_yea), 2).alias("artist_debut_year_avg_7_vs_is_explicit_artist_de"),
        F.row_number().over(w_artist_id_artist_debut_year).alias("seq_no"),
        F.min("artist_debut_year_avg_7").over(w_artist_id_artist_debut_year_2).alias("min_artist_debut_year_avg_7_so_far"),
        F.max("running_count").over(w_is_explicit).alias("is_explicit_max_running_count"),
    )
)

result = (
    with_trends
    .select(
        "id",
        "title",
        "is_explicit",
        "bpm",
        "artist_debut_year",
        "running_count",
        "is_explicit_artist_id_sum_bpm",
        "first_artist_debut_year",
        "is_explicit_artist_id_avg_artist_debut_year",
        "artist_debut_year_vs_is_explicit_artist_id_avg",
        "is_explicit_artist_id_max_bpm",
        "artist_debut_year_rank",
        "artist_debut_year_avg_7",
        "artist_debut_year_avg_7_vs_is_explicit_artist_de",
        "seq_no",
        "min_artist_debut_year_avg_7_so_far",
        "is_explicit_max_running_count",
        F.abs(F.col("artist_debut_year") - 2015).alias("artist_debut_year_distance"),
        F.coalesce(F.col("running_count"), F.lit(0)).alias("running_count_filled"),
    )
    .orderBy(F.col("is_explicit_artist_id_sum_bpm").asc())
)
