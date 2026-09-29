from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_is_explicit_by_release_date = Window.partitionBy("is_explicit").orderBy(F.col("release_date_plus_6m").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_is_explicit = Window.partitionBy("is_explicit")
w_is_explicit_by_release_date_2 = Window.partitionBy("is_explicit").orderBy(F.col("release_date_plus_6m").asc_nulls_last(), "id").rowsBetween(-2, -1)
w_is_explicit_by_release_date_3 = Window.partitionBy("is_explicit").orderBy(F.col("release_date_plus_6m").asc_nulls_last())

scoped_tracks = (
    df_tracks
    .filter((F.col("artist_id") <= 9) & (F.col("duration_sec") <= 352))
    .select(
        "id",
        "artist_id",
        "duration_sec",
        "release_date",
        "is_explicit",
    )
)

scored = (
    scoped_tracks
    .select(
        "*",
        F.add_months(F.col("release_date"), 6).alias("release_date_plus_6m"),
        F.coalesce(F.col("duration_sec"), F.lit(0)).alias("duration_sec_filled"),
        F.datediff(F.lit("2025-12-31").cast("date"), F.col("release_date")).alias("days_since_release_date"),
    )
)

metrics = (
    scored
    .select(
        "*",
        F.first("duration_sec_filled", ignorenulls=True).over(w_is_explicit_by_release_date).alias("first_known_duration_sec_filled"),
        F.round(F.avg("duration_sec_filled").over(w_is_explicit), 2).alias("is_explicit_avg_duration_sec_filled"),
        F.max("duration_sec").over(w_is_explicit_by_release_date_2).alias("duration_sec_max_2_prior"),
        F.round(F.cume_dist().over(w_is_explicit_by_release_date_3), 3).alias("release_date_plus_6m_cume_dist"),
    )
)

screened = (
    metrics.alias("s")
    .join(df_plays.alias("p"), (F.col("p.track_id") == F.col("s.id")) & (F.col("p.listener_id").isNotNull()) & (F.col("p.ms_played") <= 340114), "left_semi")
)

tagged = (
    screened
    .select(
        "id",
        "artist_id",
        "release_date",
        "is_explicit",
        "release_date_plus_6m",
        "duration_sec_filled",
        "first_known_duration_sec_filled",
        "is_explicit_avg_duration_sec_filled",
        "duration_sec_max_2_prior",
        "release_date_plus_6m_cume_dist",
        F.when(F.col("is_explicit"), "yes").otherwise("no").alias("is_explicit_label"),
    )
)

enriched_artist = (
    tagged.alias("s")
    .join(df_artists.alias("a"), (F.col("a.id") == F.col("s.artist_id")) & (F.upper(F.trim(F.col("a.artist_name"))).like("N%")), "inner")
    .select(
        "s.id",
        "s.artist_id",
        "s.release_date",
        "s.is_explicit",
        "s.first_known_duration_sec_filled",
        "s.is_explicit_avg_duration_sec_filled",
        "s.duration_sec_max_2_prior",
        "s.release_date_plus_6m_cume_dist",
        "s.is_explicit_label",
        F.col("a.debut_year").alias("artist_debut_year"),
    )
)

totals = (
    enriched_artist
    .groupBy("is_explicit")
    .agg(
        F.max("release_date").alias("last_release_date"),
        F.round(F.avg("first_known_duration_sec_filled"), 2).alias("avg_first_known_duration_sec_filled"),
        F.round(F.sum("artist_debut_year") / F.nullif(F.sum("is_explicit_avg_duration_sec_filled"), F.lit(0)), 3).alias("artist_debut_year_to_is_explicit_avg_duration_se"),
        F.count(F.when(~F.col("is_explicit"), 1)).alias("n_not_is_explicit"),
    )
)

result = (
    totals
    .select(
        "is_explicit",
        "last_release_date",
        "avg_first_known_duration_sec_filled",
        "n_not_is_explicit",
        F.last_day(F.col("last_release_date")).alias("last_release_date_month_end"),
        F.when(F.col("is_explicit"), 1).otherwise(0).alias("is_explicit_flag"),
    )
    .orderBy(F.col("last_release_date").asc_nulls_last())
)
