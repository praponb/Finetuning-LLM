from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_artist_id_by_duration_sec_ru = Window.partitionBy("artist_id").orderBy("duration_sec", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_artist_id_by_duration_sec_pr = Window.partitionBy("artist_id").orderBy("duration_sec", "id").rowsBetween(-1, -1)
w_artist_id_by_duration_sec = Window.partitionBy("artist_id").orderBy(F.col("duration_sec").desc(), "id")
w_artist_id = Window.partitionBy("artist_id")
w_all_by_duration_sec = Window.orderBy(F.col("duration_sec").asc_nulls_last(), "id")
w_artist_id_by_duration_sec_2 = Window.partitionBy("artist_id").orderBy("duration_sec", "id")
w_all_by_duration_sec_ties = Window.orderBy(F.col("duration_sec").asc_nulls_last())

valid_tracks = (
    df_tracks
    .filter(F.col("duration_sec") > 209)
    .select(
        "id",
        "artist_id",
        "title",
        "duration_sec",
        "moods",
    )
)

windowed = (
    valid_tracks
    .select(
        "id",
        "artist_id",
        "title",
        "duration_sec",
        "moods",
        F.min("duration_sec").over(w_artist_id_by_duration_sec_ru).alias("min_duration_sec_so_far"),
        F.max("duration_sec").over(w_artist_id_by_duration_sec_pr).alias("duration_sec_max_1_prior"),
        F.lag("duration_sec", 2).over(w_artist_id_by_duration_sec).alias("prev2_duration_sec"),
        F.max("duration_sec").over(w_artist_id).alias("artist_id_max_duration_sec"),
        F.lag("duration_sec").over(w_all_by_duration_sec).alias("prev_duration_sec"),
        F.lead("duration_sec", 3).over(w_artist_id_by_duration_sec_2).alias("next3_duration_sec"),
        F.row_number().over(w_all_by_duration_sec).alias("seq_no"),
        F.round(F.col("duration_sec") / F.nullif(F.sum("duration_sec").over(w_artist_id), F.lit(0)), 4).alias("duration_sec_share_of_artist_id"),
        F.ntile(10).over(w_all_by_duration_sec).alias("duration_sec_decile"),
        F.sum("duration_sec").over(w_artist_id_by_duration_sec_ru).alias("running_duration_sec"),
        F.last("duration_sec", ignorenulls=True).over(w_artist_id_by_duration_sec_ru).alias("last_known_duration_sec"),
        F.dense_rank().over(w_all_by_duration_sec_ties).alias("duration_sec_dense_rank"),
    )
)

first_rows = windowed.filter(F.col("duration_sec_dense_rank") == 1)

result = (
    first_rows
    .filter(F.col("duration_sec_share_of_artist_id") <= 0.7)
    .select(
        "id",
        "title",
        "duration_sec",
        "moods",
        "min_duration_sec_so_far",
        "prev2_duration_sec",
        "artist_id_max_duration_sec",
        "prev_duration_sec",
        "seq_no",
        "duration_sec_decile",
        "running_duration_sec",
        "last_known_duration_sec",
        "duration_sec_dense_rank",
    )
    .orderBy("duration_sec_dense_rank", "id")
    .limit(20)
)
