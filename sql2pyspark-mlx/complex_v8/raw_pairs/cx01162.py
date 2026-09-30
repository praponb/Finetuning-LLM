from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_first_played_at_to_date_sour = Window.partitionBy(F.to_date("first_played_at"), "source")
w_source_n_source_in_radio_pla = Window.partitionBy("source", "n_source_in_radio_playlist_alb").orderBy(F.col("distinct_ms_played_tiers").desc(), "source", "played_at_ym").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_source_n_source_in_radio_pla_2 = Window.partitionBy("source", "n_source_in_radio_playlist_alb").orderBy(F.col("distinct_ms_played_tiers").desc(), "source", "played_at_ym")
w_distinct_ids_n_source_in_rad = Window.partitionBy("distinct_ids", "n_source_in_radio_playlist_alb").orderBy("n_source_in_radio_playlist_alb", "source", "played_at_ym").rowsBetween(-1, Window.currentRow)
w_distinct_ids_n_source_in_rad_2 = Window.partitionBy("distinct_ids", "n_source_in_radio_playlist_alb").orderBy("n_source_in_radio_playlist_alb", "source", "played_at_ym").rowsBetween(Window.unboundedPreceding, Window.currentRow)

cleaned = (
    df_plays
    .filter((F.col("source").isin("playlist", "search", "album")) & (F.hour(F.col("played_at")).between(8, 10)))
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "ms_played",
        "device",
        "is_skipped",
        "source",
    )
)

derived = (
    cleaned
    .select(
        "id",
        "played_at",
        "device",
        "is_skipped",
        "source",
        F.initcap(F.trim(F.col("device"))).alias("device_clean"),
        F.when(F.col("ms_played") >= 236512, "A").when(F.col("ms_played") >= 73177, "B").otherwise("C").alias("ms_played_tier"),
        F.substring(F.trim(F.col("device")), 1, 3).alias("device_prefix"),
    )
)

grouped = (
    derived
    .filter(F.col("played_at") < F.lit("2025-03-08 10:00:00").cast("timestamp"))
    .groupBy("source", F.date_format("played_at", "yyyy-MM").alias("played_at_ym"))
    .agg(
        F.countDistinct("ms_played_tier").alias("distinct_ms_played_tiers"),
        F.countDistinct("source").alias("distinct_sources"),
        F.countDistinct("id").alias("distinct_ids"),
        F.count(F.when(F.col("source").isin("radio", "playlist", "album"), 1)).alias("n_source_in_radio_playlist_alb"),
        F.min("played_at").alias("first_played_at"),
    )
)

metrics = (
    grouped
    .select(
        "*",
        F.sum("distinct_ms_played_tiers").over(w_first_played_at_to_date_sour).alias("first_played_at_to_date_source_sum_distinct_ms_p"),
        F.nth_value("distinct_ms_played_tiers", 2).over(w_source_n_source_in_radio_pla).alias("second_distinct_ms_played_tiers"),
        F.max("distinct_ms_played_tiers").over(w_first_played_at_to_date_sour).alias("first_played_at_to_date_source_max_distinct_ms_p"),
        F.lag("distinct_sources").over(w_source_n_source_in_radio_pla_2).alias("prev_distinct_sources"),
        F.sum("distinct_ids").over(w_distinct_ids_n_source_in_rad).alias("distinct_ids_sum_2"),
        F.min("distinct_sources").over(w_distinct_ids_n_source_in_rad_2).alias("min_distinct_sources_so_far"),
    )
)

result = (
    metrics
    .select(
        "source",
        "played_at_ym",
        "distinct_ms_played_tiers",
        "distinct_sources",
        "distinct_ids",
        "n_source_in_radio_playlist_alb",
        "first_played_at",
        "first_played_at_to_date_source_max_distinct_ms_p",
        "distinct_ids_sum_2",
        "min_distinct_sources_so_far",
    )
    .orderBy(
        "first_played_at",
        F.col("first_played_at_to_date_source_max_distinct_ms_p").desc(),
    )
)
