from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_is_explicit_artist_id = Window.partitionBy("is_explicit", "artist_id")
w_all_by_duration_sec_ties = Window.orderBy("duration_sec")
w_all_by_duration_sec = Window.orderBy("duration_sec", "id")
w_release_date_year_artist_id = Window.partitionBy(F.year("release_date"), "artist_id").orderBy("duration_sec", "id").rowsBetween(-3, Window.currentRow)
w_release_date_year_artist_id_2 = Window.partitionBy(F.year("release_date"), "artist_id").orderBy("duration_sec", "id")
w_all_by_duration_sec_full = Window.orderBy("duration_sec", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

base_tracks = (
    df_tracks
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

windowed = (
    base_tracks
    .select(
        "*",
        F.count(F.lit(1)).over(w_is_explicit_artist_id).alias("rows_in_is_explicit_artist_id"),
        F.dense_rank().over(w_all_by_duration_sec_ties).alias("duration_sec_dense_rank"),
        (F.col("bpm") - F.lag("bpm").over(w_all_by_duration_sec)).alias("bpm_change"),
        F.sum("bpm").over(w_release_date_year_artist_id).alias("bpm_sum_4"),
        F.row_number().over(w_release_date_year_artist_id_2).alias("seq_no"),
        F.last("duration_sec").over(w_all_by_duration_sec_full).alias("last_duration_sec"),
    )
)

result = (
    windowed
    .select("id", "duration_sec", "last_duration_sec")
    .unpivot(
        ids=["id"],
        values=["duration_sec", "last_duration_sec"],
        variableColumnName="measure",
        valueColumnName="amount",
    )
    .filter(F.col("amount").isNotNull())
)
