from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_duration_sec_ties = Window.orderBy("duration_sec")
w_artist_id = Window.partitionBy("artist_id")
w_release_date_year_artist_cou = Window.partitionBy(F.year("release_date"), "artist_country").orderBy(F.col("release_date").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_bpm = Window.orderBy("bpm", "id")
w_all_by_duration_sec_full = Window.orderBy("duration_sec", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_bpm_full = Window.orderBy("bpm", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_release_date_year_artist_cou_2 = Window.partitionBy(F.year("release_date"), "artist_country").orderBy(F.col("release_date").desc())
w_artist_country_artist_id_by = Window.partitionBy("artist_country", "artist_id").orderBy("bpm", "id")
w_all_by_duration_sec_running = Window.orderBy("duration_sec", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

base = (
    df_tracks
    .filter(F.upper(F.trim(F.col("title"))).like("G%"))
    .select(
        "id",
        "artist_id",
        "title",
        "duration_sec",
        "release_date",
        "bpm",
        "moods",
        F.date_format(F.col("release_date"), "yyyy-MM-dd").alias("release_date_label"),
    )
)

with_parent = (
    base.alias("s")
    .join(df_artists.alias("a"), F.col("a.id") == F.col("s.artist_id"), "left")
    .select("s.*", "a.artist_name", F.col("a.country").alias("artist_country"))
)

sequenced = (
    with_parent
    .select(
        "*",
        F.round(F.cume_dist().over(w_all_by_duration_sec_ties), 3).alias("duration_sec_cume_dist"),
        F.max("bpm").over(w_artist_id).alias("artist_id_max_bpm"),
        F.count(F.lit(1)).over(w_artist_id).alias("rows_in_artist_id"),
        F.last("release_date").over(w_release_date_year_artist_cou).alias("last_release_date"),
        F.lag("bpm").over(w_all_by_bpm).alias("prev_bpm"),
        F.last("bpm").over(w_all_by_duration_sec_full).alias("last_bpm"),
        F.sum("bpm").over(w_artist_id).alias("artist_id_sum_bpm"),
        F.round(F.col("bpm") - F.avg("bpm").over(w_artist_id), 2).alias("bpm_vs_artist_id_avg"),
        F.size(F.collect_set("artist_country").over(w_artist_id)).alias("distinct_artist_country_in_artist_id"),
        F.first("release_date").over(w_all_by_bpm_full).alias("first_release_date"),
        F.round(F.percent_rank().over(w_release_date_year_artist_cou_2), 3).alias("release_date_percent_rank"),
        F.round(F.avg("bpm").over(w_artist_id), 2).alias("artist_id_avg_bpm"),
        F.ntile(3).over(w_artist_country_artist_id_by).alias("bpm_tercile"),
        F.last("duration_sec").over(w_release_date_year_artist_cou).alias("last_duration_sec"),
        F.count(F.lit(1)).over(w_all_by_duration_sec_running).alias("running_count"),
    )
)

labeled = (
    sequenced
    .filter(F.col("artist_id_max_bpm").between(321, 1365))
    .select(
        "id",
        "duration_sec",
        "release_date",
        "bpm",
        "release_date_label",
        "artist_country",
        "rows_in_artist_id",
        "prev_bpm",
        "last_bpm",
        "artist_id_sum_bpm",
        "bpm_vs_artist_id_avg",
        "release_date_percent_rank",
        "bpm_tercile",
        F.concat(F.col("title"), F.lit("#"), F.col("rows_in_artist_id").cast("string")).alias("title_ref"),
        F.round(F.col("bpm_vs_artist_id_avg") * 1.07, 2).alias("bpm_vs_artist_id_avg_adj"),
        F.array_contains(F.col("moods"), "focus").alias("has_focus"),
    )
)

result = (
    labeled
    .select(
        "id",
        "duration_sec",
        "release_date",
        "bpm",
        "release_date_label",
        "artist_country",
        "rows_in_artist_id",
        "prev_bpm",
        "last_bpm",
        "bpm_tercile",
        "title_ref",
        "bpm_vs_artist_id_avg_adj",
        "has_focus",
        (F.col("rows_in_artist_id") - F.col("prev_bpm")).alias("rows_in_artist_id_minus_prev_bpm"),
    )
)
