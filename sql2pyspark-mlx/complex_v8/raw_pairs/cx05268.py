from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_artist_id_artist_is_verified = Window.partitionBy("artist_id", "artist_is_verified").orderBy(F.col("artist_debut_year").asc_nulls_last(), "id")
w_artist_id_artist_is_verified_2 = Window.partitionBy("artist_id", "artist_is_verified")
w_artist_is_verified = Window.partitionBy("artist_is_verified")
w_artist_id_by_bpm_ties = Window.partitionBy("artist_id").orderBy(F.col("bpm").desc())
w_artist_id_by_bpm_running = Window.partitionBy("artist_id").orderBy(F.col("bpm").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_artist_id_by_bpm = Window.partitionBy("artist_id").orderBy("bpm", "id")
w_artist_is_verified_artist_id = Window.partitionBy("artist_is_verified", "artist_id").orderBy(F.col("artist_debut_year").desc(), "id")
w_distinct_ids = Window.partitionBy("distinct_ids")
w_all_by_bpm_change_artist_is = Window.orderBy("bpm_change_artist_is_verified_true", "bpm_decile")

scoped_tracks = (
    df_tracks
    .filter((F.col("artist_id").isNotNull()) & (F.col("bpm") > 100))
    .select(
        "id",
        "artist_id",
        "is_explicit",
        "bpm",
        "moods",
        F.abs(F.col("duration_sec") - 279).alias("duration_sec_distance"),
        F.date_format(F.col("release_date"), "MMM yyyy").alias("release_date_label"),
    )
)

enriched = (
    scoped_tracks.alias("s")
    .join(df_artists.alias("a"), (F.col("a.id") == F.col("s.artist_id")) & (F.col("a.is_verified")), "inner")
    .select(
        "s.id",
        "s.artist_id",
        "s.bpm",
        "s.moods",
        "s.duration_sec_distance",
        "s.release_date_label",
        F.col("a.is_verified").alias("artist_is_verified"),
        F.col("a.debut_year").alias("artist_debut_year"),
    )
)

ranked = (
    enriched
    .select(
        "id",
        "artist_id",
        "moods",
        "artist_is_verified",
        (F.col("duration_sec_distance") - F.lag("duration_sec_distance").over(w_artist_id_artist_is_verified)).alias("duration_sec_distance_change"),
        F.sum("bpm").over(w_artist_id_artist_is_verified_2).alias("artist_id_artist_is_verified_sum_bpm"),
        F.size(F.collect_set("artist_id").over(w_artist_is_verified)).alias("distinct_artist_id_in_artist_is_verified"),
        F.rank().over(w_artist_id_by_bpm_ties).alias("bpm_rank"),
        F.min("bpm").over(w_artist_id_artist_is_verified_2).alias("artist_id_artist_is_verified_min_bpm"),
        F.sum("artist_debut_year").over(w_artist_id_by_bpm_running).alias("running_artist_debut_year"),
        F.round(F.percent_rank().over(w_artist_id_by_bpm), 3).alias("bpm_percent_rank"),
        F.min("artist_debut_year").over(w_artist_id_artist_is_verified_2).alias("artist_id_artist_is_verified_min_artist_debut_ye"),
        (F.col("bpm") - F.lag("bpm").over(w_artist_id_artist_is_verified)).alias("bpm_change"),
        F.ntile(10).over(w_artist_id_by_bpm).alias("bpm_decile"),
        F.last("artist_debut_year", ignorenulls=True).over(w_artist_id_by_bpm_running).alias("last_known_artist_debut_year"),
        F.round(F.col("artist_debut_year") - F.avg("artist_debut_year").over(w_artist_is_verified), 2).alias("artist_debut_year_vs_artist_is_verified_avg"),
        F.sum("duration_sec_distance").over(w_artist_id_artist_is_verified_2).alias("artist_id_artist_is_verified_sum_duration_sec_di"),
        F.rank().over(w_artist_is_verified_artist_id).alias("artist_debut_year_rank"),
        F.row_number().over(w_artist_id_artist_is_verified).alias("rn"),
    )
)

enriched_2 = (
    ranked
    .select(
        "*",
        F.round(F.col("bpm_change") / F.nullif(F.col("bpm_percent_rank"), F.lit(0)), 3).alias("bpm_change_per_bpm_percent_rank"),
        F.round(F.col("rn") / F.nullif(F.col("bpm_change"), F.lit(0)), 3).alias("rn_per_bpm_change"),
    )
)

totals = (
    enriched_2
    .groupBy("bpm_decile")
    .agg(
        F.sum(F.when(F.col("artist_is_verified"), F.col("bpm_change"))).alias("bpm_change_artist_is_verified_true"),
        F.countDistinct("artist_id").alias("distinct_artist_ids"),
        F.count("*").alias("row_count"),
        F.countDistinct("id").alias("distinct_ids"),
        F.min("bpm_decile").alias("min_bpm_decile"),
        F.min("bpm_change_per_bpm_percent_rank").alias("min_bpm_change_per_bpm_percent_rank"),
    )
)

ranked_2 = (
    totals
    .select(
        "*",
        F.count(F.lit(1)).over(w_distinct_ids).alias("rows_in_distinct_ids"),
        F.lag("bpm_decile", 2).over(w_all_by_bpm_change_artist_is).alias("prev2_bpm_decile"),
    )
)

result = (
    ranked_2
    .select(
        "bpm_decile",
        "bpm_change_artist_is_verified_true",
        "distinct_artist_ids",
        "distinct_ids",
        "min_bpm_decile",
        "prev2_bpm_decile",
        (F.col("row_count") + F.col("prev2_bpm_decile")).alias("row_count_plus_prev2_bpm_decile"),
        (F.floor(F.col("row_count") / 2) * 2).alias("row_count_bucket"),
    )
)
