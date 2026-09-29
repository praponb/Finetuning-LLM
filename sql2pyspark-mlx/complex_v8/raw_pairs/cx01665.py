from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_is_skipped_track_id_by_days = Window.partitionBy("is_skipped", "track_id").orderBy("days_since_played_at_minus_max", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_played_at = Window.orderBy("played_at", "id")

recent_plays = (
    df_plays
    .filter(F.col("listener_id").isNotNull())
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "is_skipped",
        "source",
        F.concat_ws(" / ", F.col("device"), F.col("source")).alias("device_source_label"),
        F.when(F.col("source").isin("radio", "playlist"), "group_a").when(F.col("source") == "album", "group_b").otherwise("other").alias("source_group"),
        F.substring(F.trim(F.col("source")), 1, 3).alias("source_prefix"),
    )
)

annotated = (
    recent_plays
    .select(
        "*",
        F.datediff(F.lit("2025-06-30").cast("date"), F.col("played_at")).alias("days_since_played_at"),
    )
)

source_group_stats = (
    annotated
    .groupBy("source_group")
    .agg(F.max("days_since_played_at").alias("max_days_since_played_at"))
)

compared = (
    annotated.alias("s")
    .join(source_group_stats.alias("g"), F.col("g.source_group") == F.col("s.source_group"), "inner")
    .filter(F.col("s.days_since_played_at") == F.col("g.max_days_since_played_at"))
    .select(
        "s.*",
        "g.max_days_since_played_at",
        F.round(F.col("s.days_since_played_at") - F.col("g.max_days_since_played_at"), 2).alias("days_since_played_at_minus_max"),
    )
)

with_trends = (
    compared
    .select(
        "*",
        F.first("max_days_since_played_at").over(w_is_skipped_track_id_by_days).alias("first_max_days_since_played_at"),
        F.round(F.percent_rank().over(w_all_by_played_at), 3).alias("played_at_percent_rank"),
    )
)

result = (
    with_trends
    .groupingSets(
        [["source_group", "source"], ["source_group"]],
        "source_group", "source",
    )
    .agg(
        F.sum("max_days_since_played_at").alias("total_max_days_since_played_at"),
        F.count("*").alias("row_count"),
        F.array_sort(F.collect_set("source")).alias("source_list"),
    )
)
