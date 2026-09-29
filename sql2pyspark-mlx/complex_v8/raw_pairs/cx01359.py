from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_duration_sec = Window.orderBy("duration_sec", "id")
w_artist_country_is_explicit = Window.partitionBy("artist_country", "is_explicit")
w_is_explicit_by_bpm = Window.partitionBy("is_explicit").orderBy(F.col("bpm").desc(), "id")
w_artist_country = Window.partitionBy("artist_country")
w_all_by_bpm_distance_prior_3 = Window.orderBy(F.col("bpm_distance").desc(), "id").rowsBetween(-3, -1)
w_is_explicit_by_bpm_distance = Window.partitionBy("is_explicit").orderBy(F.col("bpm_distance").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_is_explicit_by_bpm_distance_2 = Window.partitionBy("is_explicit").orderBy(F.col("bpm_distance").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_is_explicit_artist_country = Window.partitionBy("is_explicit", "artist_country")
w_all_by_bpm_distance_moving_6 = Window.orderBy(F.col("bpm_distance").asc_nulls_last(), "id").rowsBetween(-6, Window.currentRow)
w_artist_country_is_explicit_b = Window.partitionBy("artist_country", "is_explicit").orderBy(F.col("bpm_max_3_prior").desc_nulls_first(), "id")
w_is_explicit_artist_id_by_dur = Window.partitionBy("is_explicit", "artist_id").orderBy("duration_sec", "id")
w_artist_id_artist_country = Window.partitionBy("artist_id", "artist_country")
w_all_by_prev_bpm = Window.orderBy(F.col("prev_bpm").asc_nulls_last(), "id")

scoped = (
    df_tracks
    .filter((F.col("bpm").between(76, 155)) & (F.col("release_date") >= F.lit("2024-10-23").cast("date")))
    .select(
        "id",
        "artist_id",
        "title",
        "duration_sec",
        "release_date",
        "is_explicit",
        "bpm",
        "moods",
        F.substring(F.trim(F.col("title")), 1, 2).alias("title_prefix"),
        F.abs(F.col("bpm") - 94).alias("bpm_distance"),
    )
)

combined_details = (
    scoped.alias("s")
    .join(df_artists.alias("a"), F.col("a.id") == F.col("s.artist_id"), "inner")
    .select(
        "s.id",
        "s.artist_id",
        "s.duration_sec",
        "s.is_explicit",
        "s.bpm",
        "s.moods",
        "s.title_prefix",
        "s.bpm_distance",
        "a.artist_name",
        F.col("a.country").alias("artist_country"),
    )
)

ranked = (
    combined_details
    .select(
        "*",
        F.lag("bpm", 1, 0).over(w_all_by_duration_sec).alias("prev_bpm"),
        F.count(F.lit(1)).over(w_artist_country_is_explicit).alias("rows_in_artist_country_is_explicit"),
        F.round(F.percent_rank().over(w_is_explicit_by_bpm), 3).alias("bpm_percent_rank"),
        F.round(F.col("bpm") / F.nullif(F.sum("bpm").over(w_artist_country), F.lit(0)), 4).alias("bpm_share_of_artist_country"),
        F.max("bpm").over(w_all_by_bpm_distance_prior_3).alias("bpm_max_3_prior"),
        F.first("duration_sec", ignorenulls=True).over(w_is_explicit_by_bpm_distance).alias("first_known_duration_sec"),
        F.nth_value("duration_sec", 2).over(w_is_explicit_by_bpm_distance_2).alias("second_duration_sec"),
        F.round(F.avg("bpm_distance").over(w_is_explicit_artist_country), 2).alias("is_explicit_artist_country_avg_bpm_distance"),
        F.round(F.avg("bpm_distance").over(w_all_by_bpm_distance_moving_6), 2).alias("bpm_distance_avg_7"),
    )
)

with_trends = (
    ranked
    .select(
        "*",
        F.ntile(4).over(w_artist_country_is_explicit_b).alias("bpm_max_3_prior_quartile"),
        F.ntile(3).over(w_is_explicit_artist_id_by_dur).alias("duration_sec_tercile"),
        (F.col("duration_sec") - F.lag("duration_sec").over(w_all_by_prev_bpm)).alias("duration_sec_change"),
        F.ntile(3).over(w_artist_country_is_explicit_b).alias("bpm_max_3_prior_tercile"),
        F.round(F.col("first_known_duration_sec") - F.avg("first_known_duration_sec").over(w_artist_id_artist_country), 2).alias("first_known_duration_sec_vs_artist_id_artist_cou"),
        F.count(F.lit(1)).over(w_artist_id_artist_country).alias("rows_in_artist_id_artist_country"),
    )
)

result = (
    with_trends
    .filter(F.col("title_prefix").isNotNull())
    .select(
        "id",
        "artist_id",
        "duration_sec",
        "is_explicit",
        "bpm",
        "moods",
        "title_prefix",
        "bpm_distance",
        "artist_name",
        "artist_country",
        "prev_bpm",
        "rows_in_artist_country_is_explicit",
        "bpm_percent_rank",
        "first_known_duration_sec",
        "second_duration_sec",
        "bpm_distance_avg_7",
        "bpm_max_3_prior_quartile",
        "duration_sec_tercile",
        "duration_sec_change",
        "bpm_max_3_prior_tercile",
        F.round(F.col("bpm_distance_avg_7") * 1.1, 2).alias("bpm_distance_avg_7_adj"),
        F.substring(F.trim(F.col("artist_name")), 1, 2).alias("artist_name_prefix"),
    )
    .orderBy("second_duration_sec", F.col("bpm_distance_avg_7").asc_nulls_last())
)
