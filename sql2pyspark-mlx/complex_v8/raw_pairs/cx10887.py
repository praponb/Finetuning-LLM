from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_artist_id_title_len_by_title = Window.partitionBy("artist_id", "title_len").orderBy("title_len", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_artist_id = Window.partitionBy("artist_id")
w_all_by_title_len_running = Window.orderBy("title_len", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_title_len = Window.orderBy("title_len", "id")
w_artist_id_title_len_by_title_2 = Window.partitionBy("artist_id", "title_len").orderBy(F.col("title_len").desc(), "id").rowsBetween(-2, -1)
w_artist_id_title_len_by_title_3 = Window.partitionBy("artist_id", "title_len").orderBy("title_len", "id").rowsBetween(-2, -1)
w_title_len = Window.partitionBy("title_len")
w_title_len_artist_id_by_bpm = Window.partitionBy("title_len", "artist_id").orderBy("bpm", "id")
w_all_by_title_len_ties = Window.orderBy("title_len")
w_artist_id_by_title_len = Window.partitionBy("artist_id").orderBy(F.col("title_len").desc(), "id")
w_artist_id_title_len_by_title_4 = Window.partitionBy("artist_id", "title_len").orderBy(F.col("title_len").desc(), "id")
w_artist_id_title_len_by_title_5 = Window.partitionBy("artist_id", "title_len").orderBy("title_len")

scoped_tracks = (
    df_tracks
    .select(
        "id",
        "artist_id",
        "title",
        "bpm",
        "moods",
        F.length(F.trim(F.col("title"))).alias("title_len"),
    )
)

with_trends = (
    scoped_tracks
    .select(
        "*",
        F.first("bpm", ignorenulls=True).over(w_artist_id_title_len_by_title).alias("first_known_bpm"),
        F.round(F.col("bpm") / F.nullif(F.sum("bpm").over(w_artist_id), F.lit(0)), 4).alias("bpm_share_of_artist_id"),
        F.first("title_len", ignorenulls=True).over(w_all_by_title_len_running).alias("first_known_title_len"),
        (F.col("bpm") - F.lag("bpm").over(w_all_by_title_len)).alias("bpm_change"),
        F.rank().over(w_all_by_title_len).alias("title_len_rank"),
        F.round(F.avg("title_len").over(w_artist_id_title_len_by_title_2), 2).alias("title_len_avg_2_prior"),
        (F.col("title_len") - F.lag("title_len").over(w_all_by_title_len)).alias("title_len_change"),
        F.max("title_len").over(w_artist_id_title_len_by_title_3).alias("title_len_max_2_prior"),
        F.round(F.col("title_len") / F.nullif(F.sum("title_len").over(w_title_len), F.lit(0)), 4).alias("title_len_share_of_title_len"),
        F.lag("bpm", 3, 0).over(w_title_len_artist_id_by_bpm).alias("prev3_bpm"),
        F.round(F.percent_rank().over(w_all_by_title_len_ties), 3).alias("title_len_percent_rank"),
        F.max("bpm").over(w_artist_id).alias("artist_id_max_bpm"),
        F.row_number().over(w_artist_id_by_title_len).alias("seq_no"),
        F.ntile(3).over(w_artist_id_title_len_by_title_4).alias("title_len_tercile"),
        F.dense_rank().over(w_artist_id_title_len_by_title_5).alias("title_len_dense_rank"),
    )
)

top_per_group = (
    with_trends
    .filter((F.col("seq_no") == 1) & (F.col("bpm_change") <= 72))
    .drop("seq_no")
)

result = (
    top_per_group
    .filter(F.col("title_len").between(3, 13))
    .select(
        "id",
        "artist_id",
        "title",
        "bpm",
        "first_known_bpm",
        "bpm_share_of_artist_id",
        "first_known_title_len",
        "title_len_rank",
        "title_len_change",
        "title_len_max_2_prior",
        "title_len_share_of_title_len",
        "prev3_bpm",
        "title_len_percent_rank",
        "title_len_tercile",
        "title_len_dense_rank",
        (F.col("bpm_change") + F.col("first_known_title_len")).alias("bpm_change_plus_first_known_title_len"),
        F.when(F.col("bpm_share_of_artist_id") >= 0.7, "HIGH").when(F.col("bpm_share_of_artist_id") >= 0.2, "MEDIUM").otherwise("LOW").alias("bpm_share_of_artist_id_tier"),
    )
    .orderBy("title_len_change", "title_len_tercile")
)
