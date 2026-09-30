from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_listener_id_prior_rows_7d_by = Window.partitionBy("listener_id", "prior_rows_7d").orderBy(F.col("played_at").desc_nulls_first(), "id")
w_listener_id_prior_rows_7d = Window.partitionBy("listener_id", "prior_rows_7d")
w_listener_id_prior_rows_7d_by_2 = Window.partitionBy("listener_id", "prior_rows_7d").orderBy(F.col("played_at").desc_nulls_first())

joined_ranges = (
    df_plays.alias("a")
    .join(
        df_plays.alias("b"),
        (F.col("b.listener_id") == F.col("a.listener_id"))
        & (F.col("b.played_at") < F.col("a.played_at"))
        & (F.col("b.played_at") >= F.col("a.played_at") - F.expr("INTERVAL 7 DAYS")),
        "left",
    )
    .groupBy("a.id", "a.listener_id", "a.played_at")
    .agg(
        F.count("b.id").alias("prior_rows_7d"),
        F.sum("b.ms_played").alias("prior_ms_played_7d"),
    )
)

without_matches = (
    joined_ranges.alias("s")
    .join(df_listeners.alias("l"), (F.col("l.id") == F.col("s.listener_id")) & (F.col("l.birth_year") <= 1988), "left_semi")
)

screened = (
    without_matches.alias("s")
    .join(df_plays.alias("p"), (F.col("p.listener_id") == F.col("s.listener_id")) & (~F.col("p.device").isin("desktop", "car")), "left_semi")
)

windowed = (
    screened
    .select(
        "id",
        F.round(F.percent_rank().over(w_listener_id_prior_rows_7d_by), 3).alias("played_at_percent_rank"),
        F.lag("prior_ms_played_7d", 1, 0).over(w_listener_id_prior_rows_7d_by).alias("prev_prior_ms_played_7d"),
        F.round(F.cume_dist().over(w_listener_id_prior_rows_7d_by_2), 3).alias("played_at_cume_dist"),
        F.rank().over(w_listener_id_prior_rows_7d_by).alias("played_at_rank"),
        F.round(F.avg("prior_rows_7d").over(w_listener_id_prior_rows_7d), 2).alias("listener_id_prior_rows_7d_avg_prior_rows_7d"),
    )
)

result = (
    windowed
    .filter(F.col("played_at_rank") > 3)
    .select(
        "id",
        "played_at_percent_rank",
        "prev_prior_ms_played_7d",
        "played_at_cume_dist",
        "played_at_rank",
    )
    .orderBy(F.col("prev_prior_ms_played_7d").desc_nulls_first(), "id")
    .limit(5)
)
