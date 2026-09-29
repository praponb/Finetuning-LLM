from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_prior_rows_7d_by_prior_ms_pl = Window.partitionBy("prior_rows_7d").orderBy(F.col("prior_ms_played_7d").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_listener_id = Window.partitionBy("listener_id")
w_prior_rows_7d_by_prior_ms_pl_2 = Window.partitionBy("prior_rows_7d").orderBy(F.col("prior_ms_played_7d").desc())

in_window = (
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

with_windows = (
    in_window
    .select(
        "*",
        F.sum("prior_rows_7d").over(w_prior_rows_7d_by_prior_ms_pl).alias("running_prior_rows_7d"),
        F.max("prior_ms_played_7d").over(w_listener_id).alias("listener_id_max_prior_ms_played_7d"),
        F.min("prior_ms_played_7d").over(w_listener_id).alias("listener_id_min_prior_ms_played_7d"),
        F.round(F.cume_dist().over(w_prior_rows_7d_by_prior_ms_pl_2), 3).alias("prior_ms_played_7d_cume_dist"),
        F.count(F.lit(1)).over(w_listener_id).alias("rows_in_listener_id"),
    )
)

result = (
    with_windows
    .cube("listener_id", "prior_rows_7d")
    .agg(
        F.sum("prior_rows_7d").alias("total_prior_rows_7d"),
        F.max("played_at").alias("last_played_at"),
    )
    .orderBy(
        F.col("listener_id").asc_nulls_last(),
        F.col("prior_rows_7d").asc_nulls_last(),
    )
)
