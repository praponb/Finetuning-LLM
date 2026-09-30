from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_release_date_running = Window.orderBy(F.col("release_date").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_artist_genre = Window.partitionBy("artist_genre")
w_all_by_release_date_moving_2 = Window.orderBy(F.col("release_date").desc(), "id").rowsBetween(-2, Window.currentRow)
w_artist_genre_by_duration_sec = Window.partitionBy("artist_genre").orderBy("duration_sec", "id")
w_artist_country_by_bpm_ties = Window.partitionBy("artist_country").orderBy(F.col("bpm").desc())
w_all_by_duration_sec_vs_artis = Window.orderBy(F.col("duration_sec_vs_artist_genre_avg").desc())
w_next3_release_date_month_rel = Window.partitionBy(F.date_trunc("month", "next3_release_date"), F.month("release_date")).orderBy(F.col("bpm_cume_dist").asc_nulls_last(), "id")
w_is_explicit_artist_country = Window.partitionBy("is_explicit", "artist_country")

filtered_tracks = (
    df_tracks
    .filter(F.col("release_date") >= F.lit("2024-06-03").cast("date"))
    .select(
        "id",
        "artist_id",
        "title",
        "duration_sec",
        "release_date",
        "is_explicit",
        "bpm",
        "moods",
        F.substring(F.trim(F.col("title")), 1, 3).alias("title_prefix"),
        F.lower(F.regexp_replace(F.trim(F.col("title")), " ", "_")).alias("title_slug"),
    )
)

enriched_artist = (
    filtered_tracks.alias("s")
    .join(df_artists.alias("a"), F.col("a.id") == F.col("s.artist_id"), "inner")
    .filter(F.col("a.genre") == "HipHop")
    .select(
        "s.id",
        "s.artist_id",
        "s.title",
        "s.duration_sec",
        "s.release_date",
        "s.is_explicit",
        "s.bpm",
        "s.moods",
        "s.title_prefix",
        "s.title_slug",
        F.col("a.country").alias("artist_country"),
        F.col("a.genre").alias("artist_genre"),
    )
)

with_windows = (
    enriched_artist
    .select(
        "*",
        F.count(F.lit(1)).over(w_all_by_release_date_running).alias("running_count"),
        F.round(F.col("duration_sec") - F.avg("duration_sec").over(w_artist_genre), 2).alias("duration_sec_vs_artist_genre_avg"),
        F.sum("duration_sec").over(w_all_by_release_date_moving_2).alias("duration_sec_sum_3"),
        F.lead("release_date", 3).over(w_artist_genre_by_duration_sec).alias("next3_release_date"),
        F.round(F.cume_dist().over(w_artist_country_by_bpm_ties), 3).alias("bpm_cume_dist"),
    )
)

metrics = (
    with_windows
    .select(
        "id",
        "bpm",
        "moods",
        "title_prefix",
        "title_slug",
        "artist_country",
        "artist_genre",
        "running_count",
        "bpm_cume_dist",
        F.rank().over(w_all_by_duration_sec_vs_artis).alias("duration_sec_vs_artist_genre_avg_rank"),
        F.row_number().over(w_next3_release_date_month_rel).alias("bpm_cume_dist_seq"),
        F.sum("bpm_cume_dist").over(w_is_explicit_artist_country).alias("is_explicit_artist_country_sum_bpm_cume_dist"),
        F.size(F.collect_set("artist_genre").over(w_is_explicit_artist_country)).alias("distinct_artist_genre_in_is_explicit_artist_coun"),
    )
)

qualified = (
    metrics
    .crossJoin(metrics.agg(F.avg("duration_sec_vs_artist_genre_avg_rank").alias("overall_avg_duration_sec_vs_artist_genre_avg_ran")))
    .filter(F.col("duration_sec_vs_artist_genre_avg_rank") > F.col("overall_avg_duration_sec_vs_artist_genre_avg_ran"))
    .drop("overall_avg_duration_sec_vs_artist_genre_avg_ran")
)

qualified_2 = (
    qualified
    .crossJoin(qualified.agg(F.avg("bpm_cume_dist").alias("overall_avg_bpm_cume_dist")))
    .filter(F.col("bpm_cume_dist") > F.col("overall_avg_bpm_cume_dist"))
    .drop("overall_avg_bpm_cume_dist")
)

result = (
    qualified_2
    .select("id", "bpm", "running_count")
    .unpivot(
        ids=["id"],
        values=["bpm", "running_count"],
        variableColumnName="metric_name",
        valueColumnName="amount",
    )
    .filter(F.col("amount").isNotNull())
)
