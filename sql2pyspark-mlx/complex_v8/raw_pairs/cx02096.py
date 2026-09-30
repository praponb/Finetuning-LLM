from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_artist_id_by_artist_debut_ye = Window.partitionBy("artist_id").orderBy(F.col("artist_debut_year").asc_nulls_last(), "id")
w_artist_id_by_artist_debut_ye_2 = Window.partitionBy("artist_id").orderBy(F.col("artist_debut_year").asc_nulls_last(), "id").rowsBetween(-1, Window.currentRow)
w_artist_id_by_artist_debut_ye_3 = Window.partitionBy("artist_id").orderBy(F.col("artist_debut_year").asc_nulls_last(), "id").rowsBetween(-3, -1)
w_all_by_artist_debut_year = Window.orderBy("artist_debut_year", "id")
w_moods_count_artist_id = Window.partitionBy("moods_count", "artist_id")
w_all_by_artist_debut_year_sha = Window.orderBy(F.col("artist_debut_year_share_of_moods_count_artist_id").desc(), "id")
w_artist_id = Window.partitionBy("artist_id")
w_all_by_artist_debut_year_sha_2 = Window.orderBy(F.col("artist_debut_year_share_of_moods_count_artist_id").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_latest = Window.partitionBy("artist_id").orderBy(F.col("days_since_prev_prev_release_date_2").desc(), F.col("id").desc())

filtered_tracks = (
    df_tracks
    .filter((F.size(F.col("moods")) >= 2) | (F.col("duration_sec") < 365))
    .select(
        "id",
        "artist_id",
        "title",
        "release_date",
        "is_explicit",
        F.size(F.col("moods")).alias("moods_count"),
    )
)

with_parent_artist = (
    filtered_tracks.alias("s")
    .join(df_artists.alias("a"), F.col("a.id") == F.col("s.artist_id"), "inner")
    .select("s.*", F.col("a.debut_year").alias("artist_debut_year"), "a.artist_name")
)

ranked = (
    with_parent_artist
    .select(
        "id",
        "artist_id",
        "title",
        "release_date",
        "artist_name",
        F.lag("release_date").over(w_artist_id_by_artist_debut_ye).alias("prev_release_date"),
        F.round(F.avg("moods_count").over(w_artist_id_by_artist_debut_ye_2), 2).alias("moods_count_avg_2"),
        F.sum("artist_debut_year").over(w_artist_id_by_artist_debut_ye_3).alias("artist_debut_year_sum_3_prior"),
        F.lag("release_date").over(w_all_by_artist_debut_year).alias("prev_release_date_2"),
        F.round(F.col("artist_debut_year") / F.nullif(F.sum("artist_debut_year").over(w_moods_count_artist_id), F.lit(0)), 4).alias("artist_debut_year_share_of_moods_count_artist_id"),
    )
)

with_trends = (
    ranked
    .select(
        "*",
        F.ntile(5).over(w_all_by_artist_debut_year_sha).alias("artist_debut_year_share_of_moods_count_artist_id_2"),
        F.round(F.col("moods_count_avg_2") - F.avg("moods_count_avg_2").over(w_artist_id), 2).alias("moods_count_avg_2_vs_artist_id_avg"),
        F.datediff(F.col("prev_release_date_2"), F.lag("prev_release_date_2").over(w_all_by_artist_debut_year_sha)).alias("days_since_prev_prev_release_date_2"),
        F.lag("prev_release_date_2", 3).over(w_all_by_artist_debut_year_sha).alias("prev3_prev_release_date_2"),
        F.row_number().over(w_all_by_artist_debut_year_sha).alias("rn"),
        F.nth_value("moods_count_avg_2", 2).over(w_all_by_artist_debut_year_sha_2).alias("second_moods_count_avg_2"),
    )
)

unique_rows = (
    with_trends
    .withColumn("row_num", F.row_number().over(w_latest))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

derived = (
    unique_rows
    .filter(F.col("moods_count_avg_2") >= 1.2)
    .select(
        "*",
        F.trunc(F.col("prev_release_date"), "MM").alias("prev_release_date_month_start"),
        F.datediff(F.col("prev_release_date"), F.col("release_date")).alias("days_release_date_to_prev_release_date"),
        F.concat_ws("|", F.col("artist_name"), F.col("title")).alias("artist_name_title_label"),
    )
)

first_rows = (
    derived
    .filter(F.col("rn") == 1)
    .drop("rn")
)

result = (
    first_rows
    .filter(F.col("artist_name_title_label").isNotNull())
    .select(
        "id",
        "artist_id",
        "title",
        "release_date",
        "artist_name",
        "prev_release_date",
        "moods_count_avg_2",
        "prev_release_date_2",
        "artist_debut_year_share_of_moods_count_artist_id",
        "moods_count_avg_2_vs_artist_id_avg",
        "prev3_prev_release_date_2",
        "second_moods_count_avg_2",
        "prev_release_date_month_start",
        "days_release_date_to_prev_release_date",
        "artist_name_title_label",
        F.round(F.col("second_moods_count_avg_2") * 0.9, 2).alias("second_moods_count_avg_2_adj"),
        F.year(F.col("prev_release_date")).alias("prev_release_date_year"),
    )
    .orderBy(F.col("title").asc_nulls_last(), "id")
    .limit(10)
)
