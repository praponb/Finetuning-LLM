from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_release_date_year_artist_id = Window.partitionBy(F.year("release_date"), "artist_id")
w_is_explicit_by_release_date = Window.partitionBy("is_explicit").orderBy(F.col("release_date").desc())
w_is_explicit_by_release_date_2 = Window.partitionBy("is_explicit").orderBy(F.col("release_date").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_artist_id_is_explicit = Window.partitionBy("artist_id", "is_explicit")
w_is_explicit_artist_id_by_dur = Window.partitionBy("is_explicit", "artist_id").orderBy("duration_sec_bucket", "id")
w_artist_id_is_explicit_by_day = Window.partitionBy("artist_id", "is_explicit").orderBy("days_since_release_date", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

scoped = (
    df_tracks
    .filter((F.array_contains(F.col("moods"), "workout")) & (F.col("bpm").between(89, 165)))
    .select(
        "id",
        "artist_id",
        "title",
        "duration_sec",
        "release_date",
        "is_explicit",
        "moods",
        (F.floor(F.col("duration_sec") / 50) * 50).alias("duration_sec_bucket"),
        F.datediff(F.lit("2026-01-01").cast("date"), F.col("release_date")).alias("days_since_release_date"),
    )
)

metrics = (
    scoped
    .select(
        "id",
        "artist_id",
        "title",
        "release_date",
        "is_explicit",
        "days_since_release_date",
        F.round(F.col("duration_sec_bucket") / F.nullif(F.sum("duration_sec_bucket").over(w_release_date_year_artist_id), F.lit(0)), 4).alias("duration_sec_bucket_share_of_release_date_year_a"),
        F.rank().over(w_is_explicit_by_release_date).alias("release_date_rank"),
        F.first("duration_sec_bucket").over(w_is_explicit_by_release_date_2).alias("first_duration_sec_bucket"),
        F.round(F.col("days_since_release_date") - F.avg("days_since_release_date").over(w_release_date_year_artist_id), 2).alias("days_since_release_date_vs_release_date_year_art"),
        F.count(F.lit(1)).over(w_artist_id_is_explicit).alias("rows_in_artist_id_is_explicit"),
        F.sum("days_since_release_date").over(w_artist_id_is_explicit).alias("artist_id_is_explicit_sum_days_since_release_dat"),
        F.lead("duration_sec", 2, 0).over(w_is_explicit_artist_id_by_dur).alias("next2_duration_sec"),
        F.first("duration_sec").over(w_artist_id_is_explicit_by_day).alias("first_duration_sec"),
        F.max("days_since_release_date").over(w_release_date_year_artist_id).alias("release_date_year_artist_id_max_days_since_relea"),
        F.round(F.col("duration_sec_bucket") - F.avg("duration_sec_bucket").over(w_artist_id_is_explicit), 2).alias("duration_sec_bucket_vs_artist_id_is_explicit_avg"),
    )
)

leaders = metrics.filter(F.col("release_date_rank") <= 3)

result = (
    leaders
    .filter(~F.col("is_explicit"))
    .select(
        "id",
        "title",
        "release_date",
        "is_explicit",
        "duration_sec_bucket_share_of_release_date_year_a",
        "first_duration_sec_bucket",
        "days_since_release_date_vs_release_date_year_art",
        "rows_in_artist_id_is_explicit",
        "artist_id_is_explicit_sum_days_since_release_dat",
        "next2_duration_sec",
        "first_duration_sec",
        "release_date_year_artist_id_max_days_since_relea",
        "duration_sec_bucket_vs_artist_id_is_explicit_avg",
        F.round(F.col("release_date_rank") / F.nullif(F.col("next2_duration_sec"), F.lit(0)), 3).alias("release_date_rank_per_next2_duration_sec"),
    )
)
