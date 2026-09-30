from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_artist_id = Window.partitionBy("artist_id")
w_release_date_year_artist_id = Window.partitionBy(F.year("release_date"), "artist_id").orderBy("duration_sec", "id")
w_artist_id_by_duration_sec_fu = Window.partitionBy("artist_id").orderBy(F.col("duration_sec").desc_nulls_first(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_artist_id_release_date_month = Window.partitionBy("artist_id", F.date_trunc("month", "release_date")).orderBy("duration_sec", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_artist_id_by_duration_sec = Window.partitionBy("artist_id").orderBy(F.col("duration_sec").desc_nulls_first(), "id")
w_release_date_year_artist_id_2 = Window.partitionBy(F.year("release_date"), "artist_id").orderBy("duration_sec")

filtered_tracks = (
    df_tracks
    .filter(F.col("duration_sec") < 409)
    .select(
        "id",
        "artist_id",
        "title",
        "duration_sec",
        "release_date",
        "bpm",
        "moods",
        F.substring(F.trim(F.col("title")), 1, 4).alias("title_prefix"),
        F.concat(F.col("title"), F.lit("#"), F.col("artist_id").cast("string")).alias("title_ref"),
    )
)

with_details = (
    filtered_tracks.alias("s")
    .join(df_artists.alias("a"), F.col("a.id") == F.col("s.artist_id"), "left")
    .select("s.*", F.col("a.debut_year").alias("artist_debut_year"))
)

ranked = (
    with_details
    .select(
        "*",
        F.round(F.avg("artist_debut_year").over(w_artist_id), 2).alias("artist_id_avg_artist_debut_year"),
        F.round(F.col("bpm") / F.nullif(F.sum("bpm").over(w_artist_id), F.lit(0)), 4).alias("bpm_share_of_artist_id"),
        (F.col("duration_sec") - F.lag("duration_sec").over(w_release_date_year_artist_id)).alias("duration_sec_change"),
        F.count(F.lit(1)).over(w_artist_id).alias("rows_in_artist_id"),
        F.first("artist_debut_year").over(w_artist_id_by_duration_sec_fu).alias("first_artist_debut_year"),
        F.first("duration_sec", ignorenulls=True).over(w_artist_id_release_date_month).alias("first_known_duration_sec"),
        F.round(F.col("bpm") - F.avg("bpm").over(w_artist_id), 2).alias("bpm_vs_artist_id_avg"),
        F.row_number().over(w_release_date_year_artist_id).alias("seq_no"),
        F.lead("release_date", 3).over(w_release_date_year_artist_id).alias("next3_release_date"),
        F.ntile(3).over(w_artist_id_by_duration_sec).alias("duration_sec_tercile"),
        F.rank().over(w_release_date_year_artist_id_2).alias("duration_sec_rank"),
        F.round(F.percent_rank().over(w_release_date_year_artist_id_2), 3).alias("duration_sec_percent_rank"),
    )
)

result = (
    ranked
    .select(
        "id",
        "artist_id",
        "title",
        "duration_sec",
        "release_date",
        "bpm",
        "title_ref",
        "artist_debut_year",
        "artist_id_avg_artist_debut_year",
        "bpm_share_of_artist_id",
        "duration_sec_change",
        "rows_in_artist_id",
        "seq_no",
        "duration_sec_tercile",
        "duration_sec_rank",
    )
    .orderBy(F.col("seq_no").desc())
)
