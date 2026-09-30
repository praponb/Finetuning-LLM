from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_is_explicit_by_release_date = Window.partitionBy("is_explicit").orderBy(F.col("release_date").desc(), "id")
w_artist_id_is_explicit_by_bpm = Window.partitionBy("artist_id", "is_explicit").orderBy(F.col("bpm").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_artist_id = Window.partitionBy("artist_id")
w_all_by_bpm = Window.orderBy("bpm", "id")
w_artist_id_by_release_date = Window.partitionBy("artist_id").orderBy(F.col("release_date").asc_nulls_last(), "id")
w_all_by_release_date = Window.orderBy("release_date", "id")
w_all_by_release_date_running = Window.orderBy("release_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

filtered = (
    df_tracks
    .filter((F.upper(F.trim(F.col("title"))).like("M%")) & (F.col("artist_id") >= 14))
    .select(
        "id",
        "artist_id",
        "title",
        "release_date",
        "is_explicit",
        "bpm",
        "moods",
        F.concat(F.col("title"), F.lit("#"), F.col("bpm").cast("string")).alias("title_ref"),
    )
)

sequenced = (
    filtered
    .select(
        "id",
        "artist_id",
        "title",
        "release_date",
        "is_explicit",
        "moods",
        "title_ref",
        F.round(F.cume_dist().over(w_is_explicit_by_release_date), 3).alias("release_date_cume_dist"),
        F.first("release_date", ignorenulls=True).over(w_artist_id_is_explicit_by_bpm).alias("first_known_release_date"),
        F.count(F.lit(1)).over(w_artist_id).alias("rows_in_artist_id"),
        F.datediff(F.col("release_date"), F.lag("release_date").over(w_is_explicit_by_release_date)).alias("days_since_prev_release_date"),
        F.round(F.cume_dist().over(w_all_by_bpm), 3).alias("bpm_cume_dist"),
        F.row_number().over(w_artist_id_by_release_date).alias("seq_no"),
        F.row_number().over(w_is_explicit_by_release_date).alias("rn"),
        F.sum("bpm").over(w_artist_id).alias("artist_id_sum_bpm"),
        F.lead("bpm", 1, 0).over(w_artist_id_by_release_date).alias("next_bpm"),
        F.row_number().over(w_all_by_release_date).alias("rn_2"),
        F.max("bpm").over(w_all_by_release_date_running).alias("max_bpm_so_far"),
        F.round(F.col("bpm") - F.avg("bpm").over(w_artist_id), 2).alias("bpm_vs_artist_id_avg"),
    )
)

result = (
    sequenced
    .select(
        "id",
        "artist_id",
        "title",
        "release_date",
        "moods",
        "title_ref",
        "release_date_cume_dist",
        "first_known_release_date",
        "rows_in_artist_id",
        "days_since_prev_release_date",
        "bpm_cume_dist",
        "seq_no",
        "rn",
        "artist_id_sum_bpm",
        "next_bpm",
        "max_bpm_so_far",
        "bpm_vs_artist_id_avg",
        F.upper(F.trim(F.col("title"))).alias("title_clean"),
        F.date_format(F.col("release_date"), "MMM yyyy").alias("release_date_label"),
    )
    .orderBy(F.col("title").asc(), "id")
    .limit(20)
)
