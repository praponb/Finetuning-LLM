from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_release_date_year_year = Window.partitionBy(F.year("release_date_year"))
w_artist_id_release_date_month = Window.partitionBy("artist_id", F.date_trunc("month", "release_date_month_start")).orderBy(F.col("release_date").desc_nulls_first(), "id")
w_release_date_year_month_by_d = Window.partitionBy(F.date_trunc("month", "release_date_year")).orderBy("duration_sec", "id")

base = (
    df_tracks
    .filter(F.col("duration_sec") >= 144)
    .select(
        "id",
        "artist_id",
        "title",
        "duration_sec",
        "release_date",
        "is_explicit",
        "moods",
        F.trunc(F.col("release_date"), "MM").alias("release_date_month_start"),
        F.date_trunc("year", F.col("release_date")).alias("release_date_year"),
    )
)

metrics = (
    base
    .select(
        "id",
        "artist_id",
        "duration_sec",
        "release_date",
        "is_explicit",
        "moods",
        "release_date_year",
        F.count(F.lit(1)).over(w_release_date_year_year).alias("rows_in_release_date_year_year"),
        F.lead("duration_sec").over(w_artist_id_release_date_month).alias("next_duration_sec"),
        F.lag("release_date_month_start", 2).over(w_release_date_year_month_by_d).alias("prev2_release_date_month_start"),
        F.lead("duration_sec").over(w_release_date_year_month_by_d).alias("next_duration_sec_2"),
    )
)

track_stats = (
    df_tracks
    .groupBy("artist_id")
    .agg(F.min("bpm").alias("min_track_bpm"))
)

summarized_rows = (
    metrics.alias("s")
    .join(track_stats.alias("t"), F.col("t.artist_id") == F.col("s.artist_id"), "left")
    .select("s.*", "t.min_track_bpm")
)

labeled = (
    summarized_rows
    .filter(F.array_contains(F.col("moods"), "chill"))
    .select(
        "id",
        "is_explicit",
        "moods",
        "release_date_year",
        "rows_in_release_date_year_year",
        "next_duration_sec",
        "prev2_release_date_month_start",
        "next_duration_sec_2",
        "min_track_bpm",
        F.to_date(F.col("release_date_year")).alias("release_date_year_date"),
        F.month(F.col("release_date")).alias("release_date_month"),
    )
)

result = (
    labeled
    .filter(F.col("release_date_month").between(4, 9))
    .select(
        "id",
        "moods",
        "release_date_year",
        "rows_in_release_date_year_year",
        "next_duration_sec_2",
        "min_track_bpm",
        "release_date_year_date",
        "release_date_month",
        (F.floor(F.col("release_date_month") / 2) * 2).alias("release_date_month_bucket"),
    )
    .orderBy(F.col("next_duration_sec_2").desc())
)
