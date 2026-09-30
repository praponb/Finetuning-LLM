from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_artist_country_by_bpm_prior = Window.partitionBy("artist_country").orderBy(F.col("bpm").desc(), "id").rowsBetween(-6, -1)
w_artist_country_by_bpm = Window.partitionBy("artist_country").orderBy(F.col("bpm").desc(), "id")
w_artist_id = Window.partitionBy("artist_id")
w_artist_country_by_bpm_runnin = Window.partitionBy("artist_country").orderBy(F.col("bpm").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_release_date_full = Window.orderBy("release_date", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_release_date = Window.orderBy("release_date", "id")
w_bpm_tercile = Window.partitionBy("bpm_tercile")
w_distinct_artist_country_in_a = Window.partitionBy("distinct_artist_country_in_artist_id").orderBy("bpm_tercile", "id").rowsBetween(-3, Window.currentRow)
w_all_by_bpm_sum_6_prior_ties = Window.orderBy(F.col("bpm_sum_6_prior").desc())
w_distinct_artist_country_in_a_2 = Window.partitionBy("distinct_artist_country_in_artist_id").orderBy("bpm_tercile", "id")
w_all_by_bpm_sum_6_prior = Window.orderBy(F.col("bpm_sum_6_prior").desc(), "id")
w_all_by_bpm_sum_6_prior_full = Window.orderBy(F.col("bpm_sum_6_prior").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

cleaned = (
    df_tracks
    .filter((F.col("bpm") <= 142) & (F.col("duration_sec") >= 164))
    .select(
        "id",
        "artist_id",
        "title",
        "release_date",
        "is_explicit",
        "bpm",
        "moods",
        F.array_join(F.array_sort(F.col("moods")), ",").alias("moods_text"),
    )
)

combined_details = (
    cleaned.alias("s")
    .join(df_artists.alias("a"), (F.col("a.id") == F.col("s.artist_id")) & (~F.col("a.is_verified")), "left")
    .select("s.*", F.col("a.country").alias("artist_country"))
)

with_trends = (
    combined_details
    .select(
        "*",
        F.sum("bpm").over(w_artist_country_by_bpm_prior).alias("bpm_sum_6_prior"),
        F.ntile(3).over(w_artist_country_by_bpm).alias("bpm_tercile"),
        F.round(F.col("bpm") - F.avg("bpm").over(w_artist_id), 2).alias("bpm_vs_artist_id_avg"),
        F.sum("bpm").over(w_artist_country_by_bpm_runnin).alias("running_bpm"),
        F.size(F.collect_set("artist_country").over(w_artist_id)).alias("distinct_artist_country_in_artist_id"),
        F.nth_value("bpm", 2).over(w_all_by_release_date_full).alias("second_bpm"),
        F.ntile(10).over(w_all_by_release_date).alias("release_date_decile"),
    )
)

tagged = (
    with_trends
    .select(
        "id",
        "bpm",
        "moods",
        "bpm_sum_6_prior",
        "bpm_tercile",
        "bpm_vs_artist_id_avg",
        "running_bpm",
        "distinct_artist_country_in_artist_id",
        F.substring(F.trim(F.col("moods_text")), 1, 3).alias("moods_text_prefix"),
    )
)

sequenced = (
    tagged
    .select(
        "*",
        F.round(F.col("bpm_vs_artist_id_avg") / F.nullif(F.sum("bpm_vs_artist_id_avg").over(w_bpm_tercile), F.lit(0)), 4).alias("bpm_vs_artist_id_avg_share_of_bpm_tercile"),
        F.round(F.avg("bpm_sum_6_prior").over(w_distinct_artist_country_in_a), 2).alias("bpm_sum_6_prior_avg_4"),
        F.round(F.percent_rank().over(w_all_by_bpm_sum_6_prior_ties), 3).alias("bpm_sum_6_prior_percent_rank"),
        F.lag("bpm_vs_artist_id_avg").over(w_distinct_artist_country_in_a_2).alias("prev_bpm_vs_artist_id_avg"),
        F.lag("bpm", 3).over(w_all_by_bpm_sum_6_prior).alias("prev3_bpm"),
        F.count(F.lit(1)).over(w_bpm_tercile).alias("rows_in_bpm_tercile"),
        F.first("bpm_tercile").over(w_all_by_bpm_sum_6_prior_full).alias("first_bpm_tercile"),
    )
)

result = (
    sequenced
    .filter(F.col("distinct_artist_country_in_artist_id") > 4)
    .select(
        "id",
        "bpm_sum_6_prior",
        "bpm_vs_artist_id_avg",
        "running_bpm",
        "distinct_artist_country_in_artist_id",
        "moods_text_prefix",
        "bpm_vs_artist_id_avg_share_of_bpm_tercile",
        "bpm_sum_6_prior_avg_4",
        "bpm_sum_6_prior_percent_rank",
        "prev_bpm_vs_artist_id_avg",
        "prev3_bpm",
        "rows_in_bpm_tercile",
        "first_bpm_tercile",
    )
    .orderBy(F.col("bpm_vs_artist_id_avg").desc())
)
