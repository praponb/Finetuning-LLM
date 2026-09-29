from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_ms_played_full = Window.orderBy(F.col("ms_played").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_listener_id_source_by_played = Window.partitionBy("listener_id", "source").orderBy("played_at", "id")
w_all_by_ms_played_running = Window.orderBy(F.col("ms_played").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_ms_played = Window.orderBy(F.col("ms_played").desc(), "id")
w_is_skipped_n_source_search = Window.partitionBy("is_skipped", "n_source_search")
w_all_by_ms_played_filled_sour = Window.orderBy(F.col("ms_played_filled_source_search").desc_nulls_first(), "is_skipped", "source").rowsBetween(-2, -1)
w_dedup = Window.partitionBy("source_or_unknown").orderBy("n_source_search_avg_2_prior", "is_skipped", "source")

valid_plays = (
    df_plays
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "ms_played",
        "device",
        "is_skipped",
        "source",
        F.coalesce(F.col("ms_played"), F.lit(0)).alias("ms_played_filled"),
        F.lower(F.regexp_replace(F.trim(F.col("source")), " ", "_")).alias("source_slug"),
    )
)

with_windows = (
    valid_plays
    .select(
        "*",
        F.last("ms_played").over(w_all_by_ms_played_full).alias("last_ms_played"),
        (F.col("ms_played") - F.lag("ms_played").over(w_listener_id_source_by_played)).alias("ms_played_change"),
        F.first("ms_played_filled", ignorenulls=True).over(w_all_by_ms_played_running).alias("first_known_ms_played_filled"),
        F.lead("ms_played_filled", 2).over(w_all_by_ms_played).alias("next2_ms_played_filled"),
    )
)

per_group = (
    with_windows
    .groupBy("is_skipped", "source")
    .agg(
        F.sum(F.when(F.col("source") == "search", F.col("ms_played_filled")).otherwise(0)).alias("ms_played_filled_source_search"),
        F.min("ms_played").alias("min_ms_played"),
        F.count(F.when(F.col("source") != "search", 1)).alias("n_source_search"),
        F.max("first_known_ms_played_filled").alias("max_first_known_ms_played_filled_check"),
    )
    .filter(F.col("max_first_known_ms_played_filled_check") > 163000)
    .drop("max_first_known_ms_played_filled_check")
)

with_trends = (
    per_group
    .select(
        "is_skipped",
        "source",
        "n_source_search",
        F.sum("n_source_search").over(w_is_skipped_n_source_search).alias("is_skipped_n_source_search_sum_n_source_search"),
        F.round(F.avg("n_source_search").over(w_all_by_ms_played_filled_sour), 2).alias("n_source_search_avg_2_prior"),
    )
)

annotated = (
    with_trends
    .select(
        "is_skipped",
        "source",
        "n_source_search_avg_2_prior",
        F.concat(F.col("source"), F.lit("#"), F.col("n_source_search").cast("string")).alias("source_ref"),
        F.when(F.col("is_skipped"), "yes").otherwise("no").alias("is_skipped_label"),
        F.coalesce(F.col("source"), F.lit("UNKNOWN")).alias("source_or_unknown"),
    )
)

latest_per_key = (
    annotated
    .withColumn("rn", F.row_number().over(w_dedup))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

labeled = (
    latest_per_key
    .filter(F.col("source_ref").isNotNull())
    .select(
        "is_skipped",
        "source",
        "n_source_search_avg_2_prior",
        "source_ref",
        "is_skipped_label",
        F.substring(F.trim(F.col("is_skipped_label")), 1, 4).alias("is_skipped_label_prefix"),
        F.when(F.col("is_skipped"), 1).otherwise(0).alias("is_skipped_flag"),
        F.coalesce(F.col("source_or_unknown"), F.lit("UNKNOWN")).alias("source_or_unknown_or_unknown"),
    )
)

result = (
    labeled
    .groupingSets(
        [["is_skipped", "is_skipped_flag", "source_or_unknown_or_unknown"], ["is_skipped"], []],
        "is_skipped", "is_skipped_flag", "source_or_unknown_or_unknown",
    )
    .agg(
        F.grouping_id().alias("gid"),
        F.countDistinct("source_or_unknown_or_unknown").alias("distinct_source_or_unknown_or_unknowns"),
    )
    .orderBy(
        F.col("is_skipped").asc_nulls_last(),
        F.col("is_skipped_flag").asc_nulls_last(),
        F.col("source_or_unknown_or_unknown").asc_nulls_last(),
    )
)
