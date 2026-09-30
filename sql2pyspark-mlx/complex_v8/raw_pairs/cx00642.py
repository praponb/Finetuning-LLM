from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_duration_sec = Window.orderBy(F.col("duration_sec").asc_nulls_last(), "id")
w_release_date_year_by_duratio = Window.partitionBy(F.year("release_date")).orderBy("duration_sec", "id").rowsBetween(-1, Window.currentRow)
w_artist_is_verified_is_explic = Window.partitionBy("artist_is_verified", "is_explicit").orderBy("release_date")
w_artist_is_verified = Window.partitionBy("artist_is_verified")
w_is_explicit_by_duration_sec = Window.partitionBy("is_explicit").orderBy(F.col("duration_sec").desc(), "id")
w_all_by_duration_sec_running = Window.orderBy(F.col("duration_sec").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_artist_is_verified_is_explic_2 = Window.partitionBy("artist_is_verified", "is_explicit").orderBy("release_date", "id")
w_artist_is_verified_is_explic_3 = Window.partitionBy("artist_is_verified", "is_explicit")

prepared = (
    df_tracks
    .filter((F.col("bpm") > 76) & (F.length(F.trim(F.col("title"))) >= 8))
    .select(
        "id",
        "artist_id",
        "title",
        "duration_sec",
        "release_date",
        "is_explicit",
        "moods",
    )
)

combined_details_artist = (
    prepared.alias("s")
    .join(df_artists.alias("a"), F.col("a.id") == F.col("s.artist_id"), "inner")
    .select(
        "s.id",
        "s.artist_id",
        "s.title",
        "s.duration_sec",
        "s.release_date",
        "s.is_explicit",
        "s.moods",
        F.col("a.is_verified").alias("artist_is_verified"),
    )
)

windowed = (
    combined_details_artist
    .select(
        "*",
        F.round(F.percent_rank().over(w_all_by_duration_sec), 3).alias("duration_sec_percent_rank"),
        F.round(F.avg("duration_sec").over(w_release_date_year_by_duratio), 2).alias("duration_sec_avg_2"),
        F.rank().over(w_artist_is_verified_is_explic).alias("release_date_rank"),
        F.round(F.col("duration_sec") / F.nullif(F.sum("duration_sec").over(w_artist_is_verified), F.lit(0)), 4).alias("duration_sec_share_of_artist_is_verified"),
        F.ntile(3).over(w_is_explicit_by_duration_sec).alias("duration_sec_tercile"),
        F.count(F.lit(1)).over(w_artist_is_verified).alias("rows_in_artist_is_verified"),
        F.round(F.percent_rank().over(w_artist_is_verified_is_explic), 3).alias("release_date_percent_rank"),
        F.round(F.avg("duration_sec").over(w_artist_is_verified), 2).alias("artist_is_verified_avg_duration_sec"),
        F.first("release_date", ignorenulls=True).over(w_all_by_duration_sec_running).alias("first_known_release_date"),
        F.lag("release_date").over(w_artist_is_verified_is_explic_2).alias("prev_release_date"),
        F.count(F.lit(1)).over(w_artist_is_verified_is_explic_3).alias("rows_in_artist_is_verified_is_explicit"),
        F.lag("release_date").over(w_is_explicit_by_duration_sec).alias("prev_release_date_2"),
        F.round(F.col("duration_sec") / F.nullif(F.sum("duration_sec").over(w_artist_is_verified_is_explic_3), F.lit(0)), 4).alias("duration_sec_share_of_artist_is_verified_is_expl"),
        F.ntile(5).over(w_all_by_duration_sec).alias("duration_sec_quintile"),
    )
)

top_rows = (
    windowed
    .filter(F.col("release_date_rank") == 1)
    .drop("release_date_rank")
)

result = (
    top_rows
    .select(
        "id",
        "artist_id",
        "title",
        "duration_sec",
        "is_explicit",
        "artist_is_verified",
        "duration_sec_avg_2",
        "duration_sec_share_of_artist_is_verified",
        "duration_sec_tercile",
        "rows_in_artist_is_verified",
        "artist_is_verified_avg_duration_sec",
        "prev_release_date",
        "rows_in_artist_is_verified_is_explicit",
        "prev_release_date_2",
        "duration_sec_quintile",
        F.round(F.col("duration_sec") / F.nullif(F.col("duration_sec_avg_2"), F.lit(0)), 3).alias("duration_sec_per_duration_sec_avg_2"),
    )
    .orderBy("rows_in_artist_is_verified_is_explicit", "id")
    .limit(20)
)
