from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_artist_id = Window.partitionBy("artist_id")
w_artist_id_release_date_quart = Window.partitionBy("artist_id", F.month("release_date_quarter")).orderBy(F.col("release_date_quarter").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_artist_id_release_date_quart_2 = Window.partitionBy("artist_id", F.year("release_date_quarter")).orderBy(F.col("bpm").desc(), "id")
w_release_date_quarter_month_b = Window.partitionBy(F.date_trunc("month", "release_date_quarter")).orderBy("bpm", "id")
w_artist_id_release_date_quart_3 = Window.partitionBy("artist_id", F.year("release_date_quarter")).orderBy(F.col("bpm").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_artist_id_by_bpm = Window.partitionBy("artist_id").orderBy(F.col("bpm").desc(), "id")

cleaned_tracks = (
    df_tracks
    .filter((F.col("artist_id").isNotNull()) | (F.col("is_explicit")))
    .select(
        "id",
        "artist_id",
        "title",
        "duration_sec",
        "bpm",
        F.date_trunc("quarter", F.col("release_date")).alias("release_date_quarter"),
        F.date_format(F.col("release_date"), "MMM yyyy").alias("release_date_label"),
    )
)

windowed = (
    cleaned_tracks
    .select(
        "*",
        F.round(F.col("bpm") / F.nullif(F.sum("bpm").over(w_artist_id), F.lit(0)), 4).alias("bpm_share_of_artist_id"),
        F.count(F.lit(1)).over(w_artist_id).alias("rows_in_artist_id"),
        F.sum("duration_sec").over(w_artist_id_release_date_quart).alias("running_duration_sec"),
        F.lag("release_date_quarter").over(w_artist_id_release_date_quart_2).alias("prev_release_date_quarter"),
        F.ntile(4).over(w_release_date_quarter_month_b).alias("bpm_quartile"),
        F.last("release_date_quarter").over(w_artist_id_release_date_quart_3).alias("last_release_date_quarter"),
        F.ntile(3).over(w_artist_id_by_bpm).alias("bpm_tercile"),
        F.max("duration_sec").over(w_artist_id).alias("artist_id_max_duration_sec"),
        F.rank().over(w_release_date_quarter_month_b).alias("bpm_rank"),
    )
)

result = (
    windowed
    .filter(F.col("artist_id").isNotNull())
    .select(
        "id",
        "artist_id",
        "title",
        "duration_sec",
        "bpm",
        "release_date_quarter",
        "release_date_label",
        "bpm_share_of_artist_id",
        "rows_in_artist_id",
        "running_duration_sec",
        "prev_release_date_quarter",
        "last_release_date_quarter",
        "bpm_tercile",
        "artist_id_max_duration_sec",
        "bpm_rank",
        F.substring(F.trim(F.col("title")), 1, 4).alias("title_prefix"),
    )
    .orderBy(F.col("title").asc_nulls_last())
)
