from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_source_by_is_skipped_flag_fu = Window.partitionBy("source").orderBy("is_skipped_flag", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_played_at = Window.orderBy("played_at", "id")
w_source = Window.partitionBy("source")

base = (
    df_plays
    .filter(F.col("played_at") >= F.lit("2025-01-31 09:00:00").cast("timestamp"))
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "ms_played",
        "is_skipped",
        "source",
        F.when(F.col("is_skipped"), 1).otherwise(0).alias("is_skipped_flag"),
        F.lower(F.regexp_replace(F.trim(F.col("source")), " ", "_")).alias("source_slug"),
        F.lower(F.regexp_replace(F.trim(F.col("source")), " ", "_")).alias("source_slug_2"),
    )
)

sequenced = (
    base
    .select(
        "*",
        F.nth_value("is_skipped_flag", 2).over(w_source_by_is_skipped_flag_fu).alias("second_is_skipped_flag"),
        F.ntile(10).over(w_all_by_played_at).alias("played_at_decile"),
        F.size(F.collect_set("listener_id").over(w_source)).alias("distinct_listener_id_in_source"),
        F.size(F.collect_set("track_id").over(w_source)).alias("distinct_track_id_in_source"),
        F.round(F.avg("ms_played").over(w_source), 2).alias("source_avg_ms_played"),
    )
)

enriched = (
    sequenced
    .filter(F.col("second_is_skipped_flag") <= 1)
    .select(
        "*",
        F.datediff(F.lit("2025-06-30").cast("date"), F.col("played_at")).alias("days_since_played_at"),
        F.to_date(F.col("played_at")).alias("played_at_date"),
    )
)

filtered = (
    enriched.alias("s")
    .join(df_plays.alias("p"), (F.col("p.track_id") == F.col("s.track_id")) & (F.col("p.device").isin("car", "phone", "tv")), "left_anti")
)

track_id_stats = (
    filtered
    .groupBy("track_id")
    .agg(F.percentile_approx("distinct_track_id_in_source", 0.5).alias("median_distinct_track_id_in_source"))
)

compared = (
    filtered.alias("s")
    .join(track_id_stats.alias("g"), F.col("g.track_id") == F.col("s.track_id"), "inner")
    .filter(F.col("s.distinct_track_id_in_source") >= F.col("g.median_distinct_track_id_in_source"))
    .select(
        "s.*",
        "g.median_distinct_track_id_in_source",
        F.round(F.col("s.distinct_track_id_in_source") - F.col("g.median_distinct_track_id_in_source"), 2).alias("distinct_track_id_in_source_minus_median"),
    )
)

result = (
    compared
    .select(
        "id",
        F.col("distinct_track_id_in_source_minus_median").alias("distinct_track_id_in_source_minus_median_v"),
        F.col("median_distinct_track_id_in_source").alias("m_median_distinct_track_id_in_source"),
        F.col("source_avg_ms_played").alias("m_source_avg_ms_played"),
    )
    .unpivot(
        ids=["id"],
        values=["distinct_track_id_in_source_minus_median_v", "m_median_distinct_track_id_in_source", "m_source_avg_ms_played"],
        variableColumnName="field",
        valueColumnName="value",
    )
    .filter(F.col("value").isNotNull())
    .orderBy("id", "field")
)
