from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_source_is_skipped_by_played = Window.partitionBy("source", "is_skipped").orderBy(F.col("played_at").desc_nulls_first())
w_source_is_skipped_by_played_2 = Window.partitionBy("source", "is_skipped").orderBy(F.col("played_at").desc_nulls_first(), "id")
w_source_by_played_at = Window.partitionBy("source").orderBy(F.col("played_at").desc(), "id")
w_source_by_played_at_ties = Window.partitionBy("source").orderBy(F.col("played_at").desc())
w_listener_id_prev_played_at_t = Window.partitionBy("listener_id", F.to_date("prev_played_at")).orderBy(F.col("played_at_percent_rank_2").desc(), "id")
w_played_at_year_track_id_by_p = Window.partitionBy(F.year("played_at"), "track_id").orderBy("played_at_percent_rank", "id").rowsBetween(-2, Window.currentRow)

scoped_plays = (
    df_plays
    .filter((F.col("ms_played").between(26344, 246779)) & (F.col("is_skipped")))
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "is_skipped",
        "source",
    )
)

with_trends = (
    scoped_plays
    .select(
        "*",
        F.round(F.percent_rank().over(w_source_is_skipped_by_played), 3).alias("played_at_percent_rank"),
        F.lag("played_at").over(w_source_is_skipped_by_played_2).alias("prev_played_at"),
        F.round((F.unix_timestamp("played_at") - F.unix_timestamp(F.lag("played_at").over(w_source_by_played_at))) / 60, 1).alias("minutes_since_prev_played_at"),
        F.round(F.percent_rank().over(w_source_by_played_at_ties), 3).alias("played_at_percent_rank_2"),
        F.ntile(10).over(w_source_by_played_at).alias("played_at_decile"),
    )
)

windowed = (
    with_trends
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "is_skipped",
        "source",
        "played_at_percent_rank",
        "prev_played_at",
        "played_at_percent_rank_2",
        "played_at_decile",
        F.lag("played_at_decile").over(w_listener_id_prev_played_at_t).alias("prev_played_at_decile"),
        F.max("played_at_percent_rank_2").over(w_played_at_year_track_id_by_p).alias("played_at_percent_rank_2_max_3"),
    )
)

derived = (
    windowed
    .filter(F.col("track_id").isNotNull())
    .select(
        "*",
        F.date_format(F.col("prev_played_at"), "yyyy-MM-dd HH:00").alias("prev_played_at_label"),
    )
)

result = (
    derived
    .groupBy("listener_id", "prev_played_at_decile")
    .pivot("source", ["playlist", "search", "album", "radio"])
    .agg(F.sum("played_at_percent_rank_2_max_3"))
    .orderBy("listener_id", "prev_played_at_decile")
)
