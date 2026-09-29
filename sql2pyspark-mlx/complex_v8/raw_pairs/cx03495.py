from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_listener_id_is_skipped_by_ms = Window.partitionBy("listener_id", "is_skipped").orderBy("ms_played", "id").rowsBetween(-6, Window.currentRow)
w_is_skipped = Window.partitionBy("is_skipped")

cleaned_plays = (
    df_plays
    .filter(F.col("track_id").isNotNull())
    .select(
        "id",
        "listener_id",
        "track_id",
        "ms_played",
        "is_skipped",
        "source",
    )
)

ranked = (
    cleaned_plays
    .select(
        "*",
        F.round(F.avg("ms_played").over(w_listener_id_is_skipped_by_ms), 2).alias("ms_played_avg_7"),
        F.round(F.col("ms_played") - F.avg("ms_played").over(w_is_skipped), 2).alias("ms_played_vs_is_skipped_avg"),
        F.size(F.collect_set("source").over(w_is_skipped)).alias("distinct_source_in_is_skipped"),
    )
)

combined_details = (
    ranked.alias("s")
    .join(df_listeners.alias("l"), F.col("l.id") == F.col("s.listener_id"), "inner")
    .select(
        "s.id",
        "s.listener_id",
        "s.track_id",
        "s.ms_played",
        "s.is_skipped",
        "s.source",
        "s.ms_played_avg_7",
        "s.distinct_source_in_is_skipped",
        F.col("l.birth_year").alias("listener_birth_year"),
    )
)

result = (
    combined_details
    .select(
        "id",
        "distinct_source_in_is_skipped",
        "ms_played",
        "listener_birth_year",
    )
    .unpivot(
        ids=["id"],
        values=["distinct_source_in_is_skipped", "ms_played", "listener_birth_year"],
        variableColumnName="metric",
        valueColumnName="amount",
    )
)
