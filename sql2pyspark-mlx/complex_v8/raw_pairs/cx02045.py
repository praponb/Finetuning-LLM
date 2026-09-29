from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_artist_is_verified_artist_id = Window.partitionBy("artist_is_verified", "artist_id").orderBy("bpm", "id").rowsBetween(-2, Window.currentRow)
w_all_by_bpm_bucket = Window.orderBy("bpm_bucket", "id")
w_artist_id = Window.partitionBy("artist_id")
w_all_by_bpm_bucket_full = Window.orderBy(F.col("bpm_bucket").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_bpm_bucket_2 = Window.orderBy(F.col("bpm_bucket").desc(), "id")
w_artist_id_artist_is_verified = Window.partitionBy("artist_id", "artist_is_verified").orderBy(F.col("bpm").desc(), "id")
w_all_by_bpm_full = Window.orderBy("bpm", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_artist_id_min_bpm = Window.orderBy(F.col("artist_id_min_bpm").desc(), "id")
w_rn = Window.partitionBy("rn")
w_rn_bpm_bucket_tercile_2_by_b = Window.partitionBy("rn", "bpm_bucket_tercile_2").orderBy("bpm_share_of_artist_id", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

base = (
    df_tracks
    .select(
        "id",
        "artist_id",
        "title",
        "bpm",
        "moods",
        (F.floor(F.col("bpm") / 25) * 25).alias("bpm_bucket"),
    )
)

combined_details = (
    base.alias("s")
    .join(df_artists.alias("a"), (F.col("a.id") == F.col("s.artist_id")) & (~F.col("a.is_verified")), "left")
    .select(
        "s.*",
        "a.artist_name",
        F.col("a.is_verified").alias("artist_is_verified"),
    )
)

ranked = (
    combined_details
    .select(
        "*",
        F.round(F.avg("bpm").over(w_artist_is_verified_artist_id), 2).alias("bpm_avg_3"),
        F.ntile(3).over(w_all_by_bpm_bucket).alias("bpm_bucket_tercile"),
        F.count(F.lit(1)).over(w_artist_id).alias("rows_in_artist_id"),
        F.min("bpm").over(w_artist_id).alias("artist_id_min_bpm"),
        F.sum("bpm_bucket").over(w_artist_id).alias("artist_id_sum_bpm_bucket"),
        F.last("bpm_bucket").over(w_all_by_bpm_bucket_full).alias("last_bpm_bucket"),
        F.ntile(3).over(w_all_by_bpm_bucket_2).alias("bpm_bucket_tercile_2"),
        F.round(F.col("bpm") / F.nullif(F.sum("bpm").over(w_artist_id), F.lit(0)), 4).alias("bpm_share_of_artist_id"),
        F.ntile(5).over(w_all_by_bpm_bucket).alias("bpm_bucket_quintile"),
        F.row_number().over(w_artist_id_artist_is_verified).alias("rn"),
        F.nth_value("bpm_bucket", 2).over(w_all_by_bpm_full).alias("second_bpm_bucket"),
    )
)

metrics = (
    ranked
    .select(
        "*",
        F.rank().over(w_all_by_artist_id_min_bpm).alias("artist_id_min_bpm_rank"),
        F.max("rows_in_artist_id").over(w_rn).alias("rn_max_rows_in_artist_id"),
        F.count(F.lit(1)).over(w_rn).alias("rows_in_rn"),
        F.sum("bpm_bucket_quintile").over(w_rn).alias("rn_sum_bpm_bucket_quintile"),
        F.size(F.collect_set("artist_id").over(w_rn)).alias("distinct_artist_id_in_rn"),
        F.min("bpm_bucket_tercile").over(w_rn).alias("rn_min_bpm_bucket_tercile"),
        F.first("bpm_bucket", ignorenulls=True).over(w_rn_bpm_bucket_tercile_2_by_b).alias("first_known_bpm_bucket"),
        F.round(F.col("bpm_bucket_tercile") - F.avg("bpm_bucket_tercile").over(w_rn), 2).alias("bpm_bucket_tercile_vs_rn_avg"),
    )
)

grouped = (
    metrics
    .filter(F.col("rows_in_rn") > 5)
    .groupBy(
        "artist_id",
        F.when(F.col("rn_max_rows_in_artist_id") >= 170, "A").when(F.col("rn_max_rows_in_artist_id") >= 71, "B").otherwise("C").alias("rn_max_rows_in_artist_id_tier"),
    )
    .agg(
        F.countDistinct("artist_id").alias("distinct_artist_ids"),
        F.count("*").alias("n_rows"),
        F.countDistinct("id").alias("distinct_ids"),
    )
)

result = (
    grouped
    .filter(F.col("rn_max_rows_in_artist_id_tier").isNotNull())
    .select(
        "artist_id",
        "rn_max_rows_in_artist_id_tier",
        "distinct_artist_ids",
        "n_rows",
        "distinct_ids",
        (F.col("distinct_ids") + F.col("distinct_artist_ids")).alias("distinct_ids_plus_distinct_artist_ids"),
        F.coalesce(F.col("distinct_ids"), F.lit(0)).alias("distinct_ids_filled"),
    )
    .orderBy(F.col("artist_id").desc_nulls_first(), F.col("n_rows").desc())
)
