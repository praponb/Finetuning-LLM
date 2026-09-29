from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_is_explicit_artist_id_by_rel = Window.partitionBy("is_explicit", "artist_id").orderBy(F.col("release_date").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_release_date_year_is_explici = Window.partitionBy(F.year("release_date"), "is_explicit")
w_is_explicit_artist_id_by_rel_2 = Window.partitionBy("is_explicit", "artist_id").orderBy(F.col("release_date").desc(), "id")
w_artist_id_by_release_date_ru = Window.partitionBy("artist_id").orderBy("release_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_artist_id = Window.partitionBy("artist_id")
w_all_by_release_date_running = Window.orderBy("release_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_artist_id_by_release_date = Window.partitionBy("artist_id").orderBy("release_date", "id")
w_is_explicit_artist_id_by_rel_3 = Window.partitionBy("is_explicit", "artist_id").orderBy(F.col("release_date").asc_nulls_last())
w_is_explicit_artist_id_by_rel_4 = Window.partitionBy("is_explicit", "artist_id").orderBy(F.col("release_date").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_duration_sec_ties = Window.orderBy("duration_sec")

base = (
    df_tracks
    .filter((F.col("artist_id") >= 6) & (F.size(F.col("moods")) >= 1))
    .select(
        "id",
        "artist_id",
        "title",
        "duration_sec",
        "release_date",
        "is_explicit",
        "moods",
        F.upper(F.trim(F.col("title"))).alias("title_clean"),
    )
)

ranked = (
    base
    .select(
        "id",
        "artist_id",
        "title",
        "duration_sec",
        "is_explicit",
        "moods",
        "title_clean",
        F.nth_value("duration_sec", 2).over(w_is_explicit_artist_id_by_rel).alias("second_duration_sec"),
        F.round(F.col("duration_sec") - F.avg("duration_sec").over(w_release_date_year_is_explici), 2).alias("duration_sec_vs_release_date_year_is_explicit_av"),
        F.row_number().over(w_is_explicit_artist_id_by_rel_2).alias("release_date_seq"),
        F.first("release_date", ignorenulls=True).over(w_artist_id_by_release_date_ru).alias("first_known_release_date"),
        F.min("duration_sec").over(w_release_date_year_is_explici).alias("release_date_year_is_explicit_min_duration_sec"),
        F.round(F.col("duration_sec") / F.nullif(F.sum("duration_sec").over(w_artist_id), F.lit(0)), 4).alias("duration_sec_share_of_artist_id"),
        F.min("duration_sec").over(w_all_by_release_date_running).alias("min_duration_sec_so_far"),
        F.row_number().over(w_artist_id_by_release_date).alias("release_date_seq_2"),
        F.rank().over(w_is_explicit_artist_id_by_rel_3).alias("release_date_rank"),
        F.count(F.lit(1)).over(w_is_explicit_artist_id_by_rel_4).alias("running_count"),
        F.round(F.cume_dist().over(w_is_explicit_artist_id_by_rel_3), 3).alias("release_date_cume_dist"),
        F.round(F.percent_rank().over(w_all_by_duration_sec_ties), 3).alias("duration_sec_percent_rank"),
        F.max("duration_sec").over(w_artist_id).alias("artist_id_max_duration_sec"),
        (F.col("duration_sec") - F.lag("duration_sec").over(w_is_explicit_artist_id_by_rel_2)).alias("duration_sec_change"),
    )
)

result = (
    ranked
    .select(
        "id",
        "duration_sec",
        "is_explicit",
        "second_duration_sec",
        "duration_sec_vs_release_date_year_is_explicit_av",
        "release_date_seq",
        "first_known_release_date",
        "release_date_year_is_explicit_min_duration_sec",
        "duration_sec_share_of_artist_id",
        "min_duration_sec_so_far",
        "release_date_rank",
        "running_count",
        "release_date_cume_dist",
        "duration_sec_percent_rank",
        "duration_sec_change",
    )
    .orderBy(F.col("min_duration_sec_so_far").asc())
)
