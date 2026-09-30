from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_duration_sec = Window.orderBy("duration_sec", "id")
w_title_len_artist_id_by_relea = Window.partitionBy("title_len", "artist_id").orderBy("release_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_title_len_artist_id_by_relea_2 = Window.partitionBy("title_len", "artist_id").orderBy("release_date", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_title_len_artist_id_by_relea_3 = Window.partitionBy("title_len", "artist_id").orderBy("release_date")
w_title_len_by_greatest_durati = Window.partitionBy("title_len").orderBy(F.col("greatest_duration_sec_bpm").desc())
w_title_len_by_release_date_de = Window.partitionBy("title_len").orderBy("release_date_dense_rank", "id").rowsBetween(-6, Window.currentRow)
w_title_len_by_greatest_durati_2 = Window.partitionBy("title_len").orderBy(F.col("greatest_duration_sec_bpm").desc(), "id")

recent_tracks = (
    df_tracks
    .filter((F.col("duration_sec").between(144, 352)) & (F.col("release_date") >= F.lit("2024-11-14").cast("date")))
    .select(
        "id",
        "artist_id",
        "title",
        "duration_sec",
        "release_date",
        "is_explicit",
        "moods",
        F.length(F.trim(F.col("title"))).alias("title_len"),
        F.greatest(F.col("duration_sec"), F.col("bpm")).alias("greatest_duration_sec_bpm"),
    )
)

sequenced = (
    recent_tracks
    .select(
        "*",
        F.lag("release_date").over(w_all_by_duration_sec).alias("prev_release_date"),
        F.sum("duration_sec").over(w_title_len_artist_id_by_relea).alias("running_duration_sec"),
        (F.col("greatest_duration_sec_bpm") - F.lag("greatest_duration_sec_bpm").over(w_all_by_duration_sec)).alias("greatest_duration_sec_bpm_change"),
        F.ntile(3).over(w_all_by_duration_sec).alias("duration_sec_tercile"),
        F.nth_value("title_len", 2).over(w_title_len_artist_id_by_relea_2).alias("second_title_len"),
        F.dense_rank().over(w_title_len_artist_id_by_relea_3).alias("release_date_dense_rank"),
    )
)

ranked = (
    sequenced
    .select(
        "id",
        "title",
        "duration_sec",
        "release_date",
        "moods",
        "title_len",
        "greatest_duration_sec_bpm",
        "running_duration_sec",
        "greatest_duration_sec_bpm_change",
        "duration_sec_tercile",
        "second_title_len",
        "release_date_dense_rank",
        F.round(F.cume_dist().over(w_title_len_by_greatest_durati), 3).alias("greatest_duration_sec_bpm_cume_dist"),
        F.round(F.avg("release_date_dense_rank").over(w_title_len_by_release_date_de), 2).alias("release_date_dense_rank_avg_7"),
        F.lead("title_len", 2).over(w_title_len_by_greatest_durati_2).alias("next2_title_len"),
    )
)

first_rows = (
    ranked
    .filter((F.col("release_date_dense_rank") <= 3) & (F.col("duration_sec").between(165, 352)))
    .drop("release_date_dense_rank")
)

result = (
    first_rows
    .groupingSets(
        [["next2_title_len", "second_title_len", "title_len"], ["next2_title_len"], ["second_title_len"], []],
        "next2_title_len", "second_title_len", "title_len",
    )
    .agg(
        F.grouping("next2_title_len").alias("is_total_next2_title_len"),
        F.max("duration_sec").alias("max_duration_sec"),
    )
    .orderBy(
        F.col("next2_title_len").asc_nulls_last(),
        F.col("second_title_len").asc_nulls_last(),
        F.col("title_len").asc_nulls_last(),
    )
)
