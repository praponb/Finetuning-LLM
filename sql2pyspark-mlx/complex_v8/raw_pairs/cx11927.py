from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_release_date_year_by_artist = Window.partitionBy(F.year("release_date")).orderBy("artist_debut_year", "id")
w_release_date_month_is_explic = Window.partitionBy(F.date_trunc("month", "release_date"), "is_explicit")
w_all_by_bpm_bucket_running = Window.orderBy(F.col("bpm_bucket").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_is_explicit_by_bpm_bucket = Window.partitionBy("is_explicit").orderBy(F.col("bpm_bucket").asc_nulls_last(), "id")
w_release_date_year_by_artist_2 = Window.partitionBy(F.year("release_date")).orderBy("artist_debut_year", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

valid = (
    df_tracks
    .filter((F.size(F.col("moods")) >= 1) & (F.upper(F.trim(F.col("title"))).like("P%")))
    .select(
        "id",
        "artist_id",
        "title",
        "duration_sec",
        "release_date",
        "is_explicit",
        F.coalesce(F.col("duration_sec"), F.lit(0)).alias("duration_sec_filled"),
        (F.floor(F.col("bpm") / 25) * 25).alias("bpm_bucket"),
    )
)

combined_details_artist = (
    valid.alias("s")
    .join(df_artists.alias("a"), F.col("a.id") == F.col("s.artist_id"), "left")
    .select(
        "s.id",
        "s.artist_id",
        "s.title",
        "s.release_date",
        "s.is_explicit",
        "s.bpm_bucket",
        F.col("a.country").alias("artist_country"),
        F.col("a.debut_year").alias("artist_debut_year"),
        "a.artist_name",
    )
)

ranked = (
    combined_details_artist
    .select(
        "*",
        F.ntile(3).over(w_release_date_year_by_artist).alias("artist_debut_year_tercile"),
        F.count(F.lit(1)).over(w_release_date_month_is_explic).alias("rows_in_release_date_month_is_explicit"),
        F.last("release_date", ignorenulls=True).over(w_all_by_bpm_bucket_running).alias("last_known_release_date"),
        F.lag("artist_debut_year").over(w_is_explicit_by_bpm_bucket).alias("prev_artist_debut_year"),
        F.round(F.col("artist_debut_year") / F.nullif(F.sum("artist_debut_year").over(w_release_date_month_is_explic), F.lit(0)), 4).alias("artist_debut_year_share_of_release_date_month_is"),
        F.round(F.avg("bpm_bucket").over(w_release_date_month_is_explic), 2).alias("release_date_month_is_explicit_avg_bpm_bucket"),
        F.last("bpm_bucket").over(w_release_date_year_by_artist_2).alias("last_bpm_bucket"),
    )
)

result = (
    ranked
    .select(
        "id",
        "release_date",
        "is_explicit",
        "bpm_bucket",
        "artist_country",
        "artist_name",
        "last_known_release_date",
        "prev_artist_debut_year",
        "artist_debut_year_share_of_release_date_month_is",
        "release_date_month_is_explicit_avg_bpm_bucket",
        "last_bpm_bucket",
        F.quarter(F.col("last_known_release_date")).alias("last_known_release_date_quarter"),
        F.coalesce(F.col("artist_debut_year"), F.lit(0)).alias("artist_debut_year_filled"),
    )
    .orderBy("prev_artist_debut_year", F.col("release_date").desc(), "id")
    .limit(20)
)
