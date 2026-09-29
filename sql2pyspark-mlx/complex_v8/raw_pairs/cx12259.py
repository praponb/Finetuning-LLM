from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_release_date_full = Window.orderBy(F.col("release_date").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_is_explicit_by_release_date = Window.partitionBy("is_explicit").orderBy("release_date")
w_is_explicit_artist_is_verifi = Window.partitionBy("is_explicit", "artist_is_verified").orderBy("release_date", "id")
w_artist_id_by_release_date = Window.partitionBy("artist_id").orderBy(F.col("release_date").asc_nulls_last(), "id")
w_release_date_month_by_releas = Window.partitionBy(F.date_trunc("month", "release_date")).orderBy(F.col("release_date").desc(), "id")
w_is_explicit_by_release_date_2 = Window.partitionBy("is_explicit").orderBy("release_date", "id")
w_is_explicit_by_release_date_3 = Window.partitionBy("is_explicit").orderBy("release_date", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_release_date = Window.orderBy(F.col("release_date").desc(), "id")

filtered_tracks = (
    df_tracks
    .filter(F.length(F.trim(F.col("title"))) >= 4)
    .select(
        "id",
        "artist_id",
        "title",
        "release_date",
        "is_explicit",
        "bpm",
    )
)

with_details = (
    filtered_tracks.alias("s")
    .join(df_artists.alias("a"), (F.col("a.id") == F.col("s.artist_id")) & (F.col("a.debut_year") <= 2006), "left")
    .select(
        "s.id",
        "s.artist_id",
        "s.release_date",
        "s.is_explicit",
        F.col("a.genre").alias("artist_genre"),
        F.col("a.is_verified").alias("artist_is_verified"),
    )
)

with_windows = (
    with_details
    .select(
        "id",
        "release_date",
        "is_explicit",
        F.nth_value("release_date", 2).over(w_all_by_release_date_full).alias("second_release_date"),
        F.rank().over(w_is_explicit_by_release_date).alias("release_date_rank"),
        F.lead("release_date").over(w_is_explicit_artist_is_verifi).alias("next_release_date"),
        F.round(F.percent_rank().over(w_is_explicit_by_release_date), 3).alias("release_date_percent_rank"),
        F.datediff(F.col("release_date"), F.lag("release_date").over(w_artist_id_by_release_date)).alias("days_since_prev_release_date"),
        F.ntile(5).over(w_release_date_month_by_releas).alias("release_date_quintile"),
        F.dense_rank().over(w_is_explicit_by_release_date_2).alias("release_date_dense_rank"),
        F.first("release_date").over(w_is_explicit_by_release_date_3).alias("first_release_date"),
        F.lag("release_date", 3).over(w_all_by_release_date).alias("prev3_release_date"),
        F.round(F.cume_dist().over(w_release_date_month_by_releas), 3).alias("release_date_cume_dist"),
    )
)

leaders = (
    with_windows
    .filter((F.col("release_date_rank") <= 3) & (F.col("second_release_date").isNotNull()))
    .drop("release_date_rank")
)

result = (
    leaders
    .select(
        "id",
        "release_date",
        "is_explicit",
        "second_release_date",
        "release_date_percent_rank",
        "days_since_prev_release_date",
        "release_date_quintile",
        "release_date_dense_rank",
        "first_release_date",
        "prev3_release_date",
    )
    .orderBy(F.col("release_date_dense_rank").desc())
)
