from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_n_upper_trim_title_like_p_by = Window.partitionBy("n_upper_trim_title_like_p").orderBy("n_upper_trim_title_like_p", "artist_id", "artist_genre")
w_n_upper_trim_title_like_p_ar = Window.partitionBy("n_upper_trim_title_like_p", "artist_id").orderBy(F.col("n_upper_trim_title_like_p").asc_nulls_last(), "artist_id", "artist_genre")
w_n_upper_trim_title_like_p_ar_2 = Window.partitionBy("n_upper_trim_title_like_p", "artist_id").orderBy(F.col("n_upper_trim_title_like_p").asc_nulls_last(), "artist_id", "artist_genre").rowsBetween(-6, Window.currentRow)
w_n_upper_trim_title_like_p_ar_3 = Window.partitionBy("n_upper_trim_title_like_p", "artist_id").orderBy(F.col("n_upper_trim_title_like_p").asc_nulls_last(), "artist_id", "artist_genre").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

valid_tracks = (
    df_tracks
    .filter((F.upper(F.trim(F.col("title"))).like("M%")) & (F.size(F.col("moods")) >= 1))
    .select(
        "id",
        "artist_id",
        "title",
        "duration_sec",
        "release_date",
        "bpm",
        F.date_add(F.col("release_date"), 30).alias("release_date_plus_30d"),
    )
)

with_parent_artist = (
    valid_tracks.alias("s")
    .join(df_artists.alias("a"), F.col("a.id") == F.col("s.artist_id"), "inner")
    .select(
        "s.id",
        "s.artist_id",
        "s.title",
        "s.bpm",
        F.col("a.genre").alias("artist_genre"),
        "a.artist_name",
    )
)

rollup_base = (
    with_parent_artist
    .filter(F.col("bpm") < 156)
    .groupBy("artist_id", "artist_genre")
    .agg(
        F.array_sort(F.collect_set("title")).alias("title_list"),
        F.count(F.when(F.upper(F.trim(F.col("title"))).like("P%"), 1)).alias("n_upper_trim_title_like_p"),
        F.percentile_approx("bpm", 0.5).alias("median_bpm"),
        F.count("*").alias("row_count"),
    )
)

with_windows = (
    rollup_base
    .select(
        "artist_id",
        "artist_genre",
        "title_list",
        "median_bpm",
        "row_count",
        F.lag("median_bpm", 3, 0).over(w_n_upper_trim_title_like_p_by).alias("prev3_median_bpm"),
        F.lag("median_bpm", 2).over(w_n_upper_trim_title_like_p_ar).alias("prev2_median_bpm"),
        F.round(F.avg("n_upper_trim_title_like_p").over(w_n_upper_trim_title_like_p_ar_2), 2).alias("n_upper_trim_title_like_p_avg_7"),
        F.last("row_count").over(w_n_upper_trim_title_like_p_ar_3).alias("last_row_count"),
    )
)

result = (
    with_windows
    .select(
        "artist_id",
        "artist_genre",
        "row_count",
        "last_row_count",
        "median_bpm",
    )
    .unpivot(
        ids=["artist_id", "artist_genre"],
        values=["row_count", "last_row_count", "median_bpm"],
        variableColumnName="metric",
        valueColumnName="amount",
    )
    .filter(F.col("amount").isNotNull())
)
