from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_is_verified_debut_year_tier = Window.partitionBy("is_verified", "debut_year_tier")
w_is_verified_label_by_debut_y = Window.partitionBy("is_verified_label").orderBy(F.col("debut_year").desc(), "id").rowsBetween(-3, -1)
w_is_verified_label_by_debut_y_2 = Window.partitionBy("is_verified_label").orderBy(F.col("debut_year").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_is_verified_label_is_verifie = Window.partitionBy("is_verified_label", "is_verified").orderBy(F.col("debut_year_vs_is_verified_debut_year_tier_avg").desc(), "id").rowsBetween(-6, Window.currentRow)
w_is_verified_label_is_verifie_2 = Window.partitionBy("is_verified_label", "is_verified").orderBy(F.col("debut_year_vs_is_verified_debut_year_tier_avg").desc(), "id")
w_is_verified = Window.partitionBy("is_verified")
w_is_verified_label_is_verifie_3 = Window.partitionBy("is_verified_label", "is_verified").orderBy(F.col("debut_year_vs_is_verified_debut_year_tier_avg").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

prepared = (
    df_artists
    .filter(F.lower(F.col("artist_name")).like("%li%"))
    .select(
        "id",
        "artist_name",
        "genre",
        "country",
        "debut_year",
        "is_verified",
        F.concat_ws(" / ", F.col("genre"), F.col("country")).alias("genre_country_label"),
        F.when(F.col("is_verified"), "yes").otherwise("no").alias("is_verified_label"),
        F.when(F.col("debut_year") >= 2018, "A").when(F.col("debut_year") >= 1997, "B").otherwise("C").alias("debut_year_tier"),
    )
)

with_windows = (
    prepared
    .select(
        "id",
        "country",
        "debut_year",
        "is_verified",
        "is_verified_label",
        "debut_year_tier",
        F.round(F.col("debut_year") - F.avg("debut_year").over(w_is_verified_debut_year_tier), 2).alias("debut_year_vs_is_verified_debut_year_tier_avg"),
        F.sum("debut_year").over(w_is_verified_label_by_debut_y).alias("debut_year_sum_3_prior"),
        F.last("debut_year").over(w_is_verified_label_by_debut_y_2).alias("last_debut_year"),
        F.size(F.collect_set("genre").over(w_is_verified_debut_year_tier)).alias("distinct_genre_in_is_verified_debut_year_tier"),
    )
)

sequenced = (
    with_windows
    .select(
        "id",
        "country",
        "debut_year",
        "debut_year_tier",
        "debut_year_vs_is_verified_debut_year_tier_avg",
        "debut_year_sum_3_prior",
        "distinct_genre_in_is_verified_debut_year_tier",
        F.sum("last_debut_year").over(w_is_verified_label_is_verifie).alias("last_debut_year_sum_7"),
        (F.col("last_debut_year") - F.lag("last_debut_year").over(w_is_verified_label_is_verifie_2)).alias("last_debut_year_change"),
        F.round(F.col("debut_year") / F.nullif(F.sum("debut_year").over(w_is_verified), F.lit(0)), 4).alias("debut_year_share_of_is_verified"),
        F.count(F.lit(1)).over(w_is_verified_label_is_verifie_3).alias("running_count"),
        F.ntile(10).over(w_is_verified_label_is_verifie_2).alias("debut_year_vs_is_verified_debut_year_tier_avg_de"),
    )
)

debut_year_tier_stats = (
    sequenced
    .groupBy("debut_year_tier")
    .agg(F.max("distinct_genre_in_is_verified_debut_year_tier").alias("max_distinct_genre_in_is_verified_debut_year_tie"))
)

outliers = (
    sequenced.alias("s")
    .join(debut_year_tier_stats.alias("g"), F.col("g.debut_year_tier") == F.col("s.debut_year_tier"), "inner")
    .filter(F.col("s.distinct_genre_in_is_verified_debut_year_tier") == F.col("g.max_distinct_genre_in_is_verified_debut_year_tie"))
    .select(
        "s.*",
        "g.max_distinct_genre_in_is_verified_debut_year_tie",
        F.round(F.col("s.distinct_genre_in_is_verified_debut_year_tier") - F.col("g.max_distinct_genre_in_is_verified_debut_year_tie"), 2).alias("distinct_genre_in_is_verified_debut_year_tier_mi"),
    )
)

detail_rows_tracks = (
    outliers.alias("s")
    .join(df_tracks.alias("t"), (F.col("t.artist_id") == F.col("s.id")) & (F.col("t.bpm") >= 76), "inner")
    .select(
        "s.id",
        "s.country",
        "s.debut_year",
        "s.debut_year_tier",
        "s.debut_year_vs_is_verified_debut_year_tier_avg",
        "s.debut_year_sum_3_prior",
        "s.distinct_genre_in_is_verified_debut_year_tier",
        "s.last_debut_year_sum_7",
        "s.last_debut_year_change",
        "s.running_count",
        "s.debut_year_vs_is_verified_debut_year_tier_avg_de",
        "s.max_distinct_genre_in_is_verified_debut_year_tie",
        "s.distinct_genre_in_is_verified_debut_year_tier_mi",
        F.col("t.id").alias("track_id"),
        F.col("t.is_explicit").alias("track_is_explicit"),
        F.col("t.release_date").alias("track_release_date"),
        F.col("t.bpm").alias("track_bpm"),
        F.col("t.title").alias("track_title"),
    )
)

track_stats = (
    df_tracks
    .groupBy("artist_id")
    .agg(
        F.count("*").alias("track_count"),
        F.max("duration_sec").alias("max_track_duration_sec"),
        F.max("release_date").alias("last_track_release_date"),
    )
)

summarized_rows = (
    detail_rows_tracks.alias("s")
    .join(track_stats.alias("t"), F.col("t.artist_id") == F.col("s.id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("t.track_count"), F.lit(0)).alias("track_count"),
        "t.max_track_duration_sec",
        "t.last_track_release_date",
    )
)

result = (
    summarized_rows
    .select(
        "id",
        "country",
        "debut_year",
        "debut_year_tier",
        "debut_year_vs_is_verified_debut_year_tier_avg",
        "debut_year_sum_3_prior",
        "last_debut_year_sum_7",
        "last_debut_year_change",
        "running_count",
        "debut_year_vs_is_verified_debut_year_tier_avg_de",
        "max_distinct_genre_in_is_verified_debut_year_tie",
        "distinct_genre_in_is_verified_debut_year_tier_mi",
        "track_id",
        "track_release_date",
        "track_bpm",
        "track_title",
        "track_count",
        "last_track_release_date",
    )
    .orderBy("debut_year_tier", "id", "track_id")
    .limit(10)
)
