from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_play_played_at = Window.orderBy("play_played_at", "id", "play_id")
w_play_played_at_year_prior_ro = Window.partitionBy(F.year("play_played_at"), "prior_rows_3d").orderBy("prior_rows_3d", "id", "play_id")

covered = (
    df_plays.alias("a")
    .join(
        df_plays.alias("b"),
        (F.col("b.listener_id") == F.col("a.listener_id"))
        & (F.col("b.played_at") < F.col("a.played_at"))
        & (F.col("b.played_at") >= F.col("a.played_at") - F.expr("INTERVAL 3 DAYS")),
        "left",
    )
    .groupBy("a.id", "a.listener_id", "a.played_at")
    .agg(
        F.count("b.id").alias("prior_rows_3d"),
        F.sum("b.ms_played").alias("prior_ms_played_3d"),
    )
)

line_level_plays = (
    covered.alias("s")
    .join(df_plays.alias("p"), F.col("p.listener_id") == F.col("s.listener_id"), "left")
    .select(
        "s.id",
        "s.listener_id",
        "s.prior_rows_3d",
        F.col("p.id").alias("play_id"),
        F.col("p.played_at").alias("play_played_at"),
        F.col("p.source").alias("play_source"),
    )
)

with_trends = (
    line_level_plays
    .select(
        "id",
        "listener_id",
        "prior_rows_3d",
        "play_id",
        "play_played_at",
        F.round((F.unix_timestamp("play_played_at") - F.unix_timestamp(F.lag("play_played_at").over(w_all_by_play_played_at))) / 60, 1).alias("minutes_since_prev_play_played_at"),
        F.lag("prior_rows_3d", 3).over(w_play_played_at_year_prior_ro).alias("prev3_prior_rows_3d"),
    )
)

result = (
    with_trends
    .select(
        "id",
        "listener_id",
        "prior_rows_3d",
        "play_id",
        "play_played_at",
        "minutes_since_prev_play_played_at",
        F.date_format(F.col("play_played_at"), "yyyy-MM-dd HH:00").alias("play_played_at_label"),
    )
    .orderBy("play_id")
)
