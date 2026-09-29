from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_last_track_release_date_year = Window.partitionBy(F.year("last_track_release_date"))
w_last_track_release_date_mont = Window.partitionBy(F.month("last_track_release_date")).orderBy("first_track_release_date", "id")
w_first_track_release_date_mon = Window.partitionBy(F.month("first_track_release_date")).orderBy(F.col("debut_year").desc(), "id").rowsBetween(-2, -1)
w_genre_by_last_track_release = Window.partitionBy("genre").orderBy(F.col("last_track_release_date").desc(), "id")
w_last_track_release_date_mont_2 = Window.partitionBy(F.month("last_track_release_date"), F.month("first_track_release_date"))

scoped_artists = (
    df_artists
    .filter(F.col("country") == "BR")
    .select(
        "id",
        "artist_name",
        "genre",
        "debut_year",
    )
)

without_matches = (
    scoped_artists.alias("s")
    .join(df_tracks.alias("t"), (F.col("t.artist_id") == F.col("s.id")) & (F.col("t.bpm") >= 94) & (F.length(F.trim(F.col("t.title"))) >= 6), "left_anti")
)

track_stats = (
    df_tracks
    .groupBy("artist_id")
    .agg(
        F.min("release_date").alias("first_track_release_date"),
        F.max("release_date").alias("last_track_release_date"),
    )
)

profiled = (
    without_matches.alias("s")
    .join(track_stats.alias("t"), F.col("t.artist_id") == F.col("s.id"), "left")
    .select("s.*", "t.first_track_release_date", "t.last_track_release_date")
)

metrics = (
    profiled
    .select(
        "*",
        F.count(F.lit(1)).over(w_last_track_release_date_year).alias("rows_in_last_track_release_date_year"),
        F.datediff(F.col("first_track_release_date"), F.lag("first_track_release_date").over(w_last_track_release_date_mont)).alias("days_since_prev_first_track_release_date"),
        F.round(F.col("debut_year") / F.nullif(F.sum("debut_year").over(w_last_track_release_date_year), F.lit(0)), 4).alias("debut_year_share_of_last_track_release_date_year"),
        F.max("debut_year").over(w_first_track_release_date_mon).alias("debut_year_max_2_prior"),
    )
)

genre_stats = (
    metrics
    .groupBy("genre")
    .agg(F.percentile_approx("debut_year", 0.5).alias("median_debut_year"))
)

outliers = (
    metrics.alias("s")
    .join(genre_stats.alias("g"), F.col("g.genre") == F.col("s.genre"), "inner")
    .filter(F.col("s.debut_year") >= F.col("g.median_debut_year"))
    .select(
        "s.*",
        "g.median_debut_year",
        F.round(F.col("s.debut_year") - F.col("g.median_debut_year"), 2).alias("debut_year_minus_median"),
    )
)

windowed = (
    outliers
    .select(
        "*",
        F.ntile(3).over(w_genre_by_last_track_release).alias("last_track_release_date_tercile"),
        F.round(F.col("debut_year") / F.nullif(F.sum("debut_year").over(w_last_track_release_date_mont_2), F.lit(0)), 4).alias("debut_year_share_of_last_track_release_date_mont"),
        F.size(F.collect_set("genre").over(w_last_track_release_date_mont_2)).alias("distinct_genre_in_last_track_release_date_month_"),
        F.round(F.avg("debut_year_minus_median").over(w_last_track_release_date_mont_2), 2).alias("last_track_release_date_month_first_track_releas"),
        F.round(F.avg("debut_year_max_2_prior").over(w_last_track_release_date_mont_2), 2).alias("last_track_release_date_month_first_track_releas_2"),
    )
)

result = (
    windowed
    .select(
        "id",
        "artist_name",
        "first_track_release_date",
        "days_since_prev_first_track_release_date",
        "debut_year_max_2_prior",
        "median_debut_year",
        "last_track_release_date_tercile",
        "debut_year_share_of_last_track_release_date_mont",
        "last_track_release_date_month_first_track_releas",
        F.when(F.col("genre").isin("Classical", "HipHop"), "group_a").when(F.col("genre") == "Indie", "group_b").otherwise("other").alias("genre_group"),
    )
    .orderBy("debut_year_share_of_last_track_release_date_mont")
)
