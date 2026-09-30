from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_played_at_month_listener_id = Window.partitionBy(F.month("played_at"), "listener_id").orderBy("played_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_played_at_month_listener_id_2 = Window.partitionBy(F.month("played_at"), "listener_id").orderBy("played_at", "id")

cleaned_plays = (
    df_plays
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "ms_played",
        "is_skipped",
        F.lower(F.trim(F.col("device"))).alias("device_clean"),
        F.initcap(F.trim(F.col("source"))).alias("source_clean"),
    )
)

windowed = (
    cleaned_plays
    .select(
        "id",
        "track_id",
        "played_at",
        "is_skipped",
        "device_clean",
        "source_clean",
        F.last("ms_played", ignorenulls=True).over(w_played_at_month_listener_id).alias("last_known_ms_played"),
        F.lag("played_at").over(w_played_at_month_listener_id_2).alias("prev_played_at"),
        F.lead("played_at").over(w_played_at_month_listener_id_2).alias("next_played_at"),
    )
)

with_children_plays = (
    windowed.alias("s")
    .join(df_plays.alias("p"), F.col("p.track_id") == F.col("s.track_id"), "left")
    .select(
        "s.id",
        "s.track_id",
        "s.is_skipped",
        "s.source_clean",
        "s.next_played_at",
        F.col("p.id").alias("play_id"),
        F.col("p.played_at").alias("play_played_at"),
        F.col("p.ms_played").alias("play_ms_played"),
    )
)

result = (
    with_children_plays
    .select(
        "id",
        "track_id",
        "is_skipped",
        "source_clean",
        "next_played_at",
        "play_id",
        "play_played_at",
        "play_ms_played",
        F.concat(F.col("source_clean"), F.lit("#"), F.col("id").cast("string")).alias("source_clean_ref"),
    )
    .orderBy(
        F.col("source_clean_ref").desc(),
        F.col("source_clean").desc_nulls_first(),
    )
)
