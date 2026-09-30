from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_is_explicit_release_date_mon = Window.partitionBy("is_explicit", F.date_trunc("month", "release_date")).orderBy(F.col("release_date_quarter").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_artist_id_release_date_month = Window.partitionBy("artist_id", F.date_trunc("month", "release_date")).orderBy("release_date", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_is_explicit_by_release_date = Window.partitionBy("is_explicit").orderBy("release_date")
w_artist_id_release_date_month_2 = Window.partitionBy("artist_id", F.date_trunc("month", "release_date")).orderBy("release_date", "id")
w_is_explicit_by_release_date_2 = Window.partitionBy("is_explicit").orderBy(F.col("release_date_quarter").asc_nulls_last())
w_is_explicit_release_date_mon_2 = Window.partitionBy("is_explicit", F.date_trunc("month", "release_date")).orderBy(F.col("release_date_quarter").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_artist_id_release_date_month_3 = Window.partitionBy("artist_id", F.date_trunc("month", "release_date")).orderBy("release_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_artist_is_verified_release_d = Window.partitionBy("artist_is_verified", "release_date_dense_rank_2")
w_release_date_dense_rank_2_by = Window.partitionBy("release_date_dense_rank_2").orderBy("last_known_release_date", "id")
w_release_date_dense_rank_by_l = Window.partitionBy("release_date_dense_rank").orderBy("last_release_date", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

base_tracks = (
    df_tracks
    .filter((F.col("duration_sec") >= 209) & (F.year(F.col("release_date")) == 2025))
    .select(
        "id",
        "artist_id",
        "duration_sec",
        "release_date",
        "is_explicit",
        "bpm",
        F.date_trunc("quarter", F.col("release_date")).alias("release_date_quarter"),
        F.dayofweek(F.col("release_date")).alias("release_date_dayofweek"),
    )
)

combined_details_artist = (
    base_tracks.alias("s")
    .join(df_artists.alias("a"), (F.col("a.id") == F.col("s.artist_id")) & (F.col("a.is_verified")), "inner")
    .select(
        "s.id",
        "s.artist_id",
        "s.release_date",
        "s.is_explicit",
        "s.release_date_quarter",
        F.col("a.is_verified").alias("artist_is_verified"),
        F.col("a.country").alias("artist_country"),
        "a.artist_name",
    )
)

metrics = (
    combined_details_artist
    .select(
        "id",
        "artist_id",
        "release_date",
        "is_explicit",
        "release_date_quarter",
        "artist_is_verified",
        "artist_name",
        F.last("release_date", ignorenulls=True).over(w_is_explicit_release_date_mon).alias("last_known_release_date"),
        F.last("release_date").over(w_artist_id_release_date_month).alias("last_release_date"),
        F.dense_rank().over(w_is_explicit_by_release_date).alias("release_date_dense_rank"),
        F.row_number().over(w_artist_id_release_date_month_2).alias("seq_no"),
        F.round(F.percent_rank().over(w_is_explicit_by_release_date_2), 3).alias("release_date_quarter_percent_rank"),
        F.nth_value("release_date", 2).over(w_is_explicit_release_date_mon_2).alias("second_release_date"),
        F.first("release_date", ignorenulls=True).over(w_artist_id_release_date_month_3).alias("first_known_release_date"),
        F.round(F.percent_rank().over(w_artist_id_release_date_month_2), 3).alias("release_date_percent_rank"),
        F.dense_rank().over(w_artist_id_release_date_month_2).alias("release_date_dense_rank_2"),
        F.round(F.cume_dist().over(w_is_explicit_by_release_date), 3).alias("release_date_cume_dist"),
    )
)

windowed = (
    metrics
    .select(
        "*",
        F.round(F.col("release_date_dense_rank") - F.avg("release_date_dense_rank").over(w_artist_is_verified_release_d), 2).alias("release_date_dense_rank_vs_artist_is_verified_re"),
        F.lag("release_date", 3).over(w_release_date_dense_rank_2_by).alias("prev3_release_date"),
        F.round(F.col("release_date_dense_rank_2") - F.avg("release_date_dense_rank_2").over(w_artist_is_verified_release_d), 2).alias("release_date_dense_rank_2_vs_artist_is_verified_"),
        F.first("release_date_quarter").over(w_release_date_dense_rank_by_l).alias("first_release_date_quarter"),
        F.count(F.lit(1)).over(w_artist_is_verified_release_d).alias("rows_in_artist_is_verified_release_date_dense_ra"),
    )
)

result = (
    windowed
    .filter(F.col("first_release_date_quarter") >= F.lit("2025-01-21 09:00:00").cast("timestamp"))
    .select(
        "id",
        "artist_id",
        "release_date",
        "is_explicit",
        "artist_is_verified",
        "artist_name",
        "last_known_release_date",
        "last_release_date",
        "release_date_dense_rank",
        "second_release_date",
        "first_known_release_date",
        "release_date_dense_rank_2",
        "release_date_cume_dist",
        "release_date_dense_rank_vs_artist_is_verified_re",
        "prev3_release_date",
        "release_date_dense_rank_2_vs_artist_is_verified_",
        "first_release_date_quarter",
    )
    .orderBy(F.col("artist_id").desc())
)
