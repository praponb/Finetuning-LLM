from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_artist_country = Window.partitionBy("artist_country")
w_all_by_artist_debut_year_run = Window.orderBy("artist_debut_year", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_artist_id_by_bpm = Window.partitionBy("artist_id").orderBy(F.col("bpm").desc(), "id")
w_all_by_bpm_moving_2 = Window.orderBy("bpm", "id").rowsBetween(-2, Window.currentRow)
w_all_by_artist_debut_year_tie = Window.orderBy("artist_debut_year")
w_all_by_artist_debut_year = Window.orderBy("artist_debut_year", "id")
w_artist_is_verified_artist_co = Window.partitionBy("artist_is_verified", "artist_country").orderBy(F.col("duration_sec").desc(), "id")
w_artist_id_artist_country = Window.partitionBy("artist_id", "artist_country")
w_all_by_bpm = Window.orderBy("bpm", "id")

filtered_tracks = (
    df_tracks
    .filter((~F.array_contains(F.col("moods"), "chill")) & (F.col("artist_id") <= 14))
    .select(
        "id",
        "artist_id",
        "title",
        "duration_sec",
        "is_explicit",
        "bpm",
        "moods",
    )
)

joined_artist = (
    filtered_tracks.alias("s")
    .join(df_artists.alias("a"), F.col("a.id") == F.col("s.artist_id"), "inner")
    .filter(~F.col("a.is_verified"))
    .select(
        "s.id",
        "s.artist_id",
        "s.title",
        "s.duration_sec",
        "s.bpm",
        "s.moods",
        F.col("a.is_verified").alias("artist_is_verified"),
        F.col("a.country").alias("artist_country"),
        F.col("a.debut_year").alias("artist_debut_year"),
    )
)

metrics = (
    joined_artist
    .select(
        "*",
        F.count(F.lit(1)).over(w_artist_country).alias("rows_in_artist_country"),
        F.last("duration_sec", ignorenulls=True).over(w_all_by_artist_debut_year_run).alias("last_known_duration_sec"),
        F.round(F.cume_dist().over(w_artist_id_by_bpm), 3).alias("bpm_cume_dist"),
        F.lag("duration_sec", 1, 0).over(w_artist_id_by_bpm).alias("prev_duration_sec"),
        F.max("duration_sec").over(w_all_by_bpm_moving_2).alias("duration_sec_max_3"),
        F.max("artist_debut_year").over(w_artist_country).alias("artist_country_max_artist_debut_year"),
        F.size(F.collect_set("artist_id").over(w_artist_country)).alias("distinct_artist_id_in_artist_country"),
        F.round(F.col("artist_debut_year") / F.nullif(F.sum("artist_debut_year").over(w_artist_country), F.lit(0)), 4).alias("artist_debut_year_share_of_artist_country"),
        F.round(F.cume_dist().over(w_all_by_artist_debut_year_tie), 3).alias("artist_debut_year_cume_dist"),
        F.round(F.avg("bpm").over(w_artist_country), 2).alias("artist_country_avg_bpm"),
        (F.col("bpm") - F.lag("bpm").over(w_all_by_artist_debut_year)).alias("bpm_change"),
        F.ntile(3).over(w_artist_is_verified_artist_co).alias("duration_sec_tercile"),
        F.round(F.col("duration_sec") - F.avg("duration_sec").over(w_artist_id_artist_country), 2).alias("duration_sec_vs_artist_id_artist_country_avg"),
        F.round(F.avg("artist_debut_year").over(w_artist_id_artist_country), 2).alias("artist_id_artist_country_avg_artist_debut_year"),
        F.rank().over(w_all_by_bpm).alias("bpm_rank"),
    )
)

totals = (
    metrics
    .filter(F.col("artist_id") <= 8)
    .groupBy("artist_is_verified", "artist_id")
    .agg(
        F.count("*").alias("n_rows"),
        F.sum("distinct_artist_id_in_artist_country").alias("total_distinct_artist_id_in_artist_country"),
        F.countDistinct("artist_country").alias("distinct_artist_countrys"),
        F.round(F.sum("bpm_rank") / F.nullif(F.sum("artist_debut_year_share_of_artist_country"), F.lit(0)), 3).alias("bpm_rank_to_artist_debut_year_share_of_artist_co"),
        F.sum(F.when(~F.col("artist_is_verified"), F.col("artist_debut_year")).otherwise(0)).alias("artist_debut_year_artist_is_verified_false"),
        F.max("artist_debut_year_share_of_artist_country").alias("max_artist_debut_year_share_of_artist_country"),
    )
)

result = (
    totals
    .select(
        "artist_is_verified",
        "artist_id",
        "n_rows",
        "total_distinct_artist_id_in_artist_country",
        "distinct_artist_countrys",
        "bpm_rank_to_artist_debut_year_share_of_artist_co",
        "max_artist_debut_year_share_of_artist_country",
        F.round(F.col("distinct_artist_countrys") / F.nullif(F.col("n_rows"), F.lit(0)), 3).alias("distinct_artist_countrys_per_n_rows"),
    )
    .orderBy(F.col("artist_id").desc(), F.col("n_rows").desc())
)
