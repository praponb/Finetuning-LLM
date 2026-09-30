from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_prior_rows_3d_listener_id = Window.partitionBy("prior_rows_3d", "listener_id")
w_all_by_prior_rows_3d_full = Window.orderBy(F.col("prior_rows_3d").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_prior_rows_3d_ties = Window.orderBy(F.col("prior_rows_3d").desc())
w_all_by_prior_rows_3d_running = Window.orderBy(F.col("prior_rows_3d").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_prior_rows_3d_prior_3 = Window.orderBy(F.col("prior_rows_3d").desc(), "id").rowsBetween(-3, -1)

in_window = (
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

with_parent_listener = (
    in_window.alias("s")
    .join(df_listeners.alias("l"), F.col("l.id") == F.col("s.listener_id"), "left")
    .select(
        "s.id",
        "s.listener_id",
        "s.played_at",
        "s.prior_rows_3d",
        F.col("l.interests").alias("listener_interests"),
    )
)

with_activity = (
    with_parent_listener.alias("s")
    .join(df_plays.alias("p"), (F.col("p.listener_id") == F.col("s.listener_id")) & (F.col("p.track_id").isNotNull()), "left_anti")
    .filter(F.col("listener_id").isNotNull())
)

sequenced = (
    with_activity
    .select(
        "id",
        "listener_id",
        "played_at",
        "prior_rows_3d",
        "listener_interests",
        F.nth_value("prior_rows_3d", 2).over(w_all_by_prior_rows_3d_full).alias("second_prior_rows_3d"),
        F.round(F.percent_rank().over(w_all_by_prior_rows_3d_ties), 3).alias("prior_rows_3d_percent_rank"),
        F.round(F.col("prior_rows_3d") - F.avg("prior_rows_3d").over(w_prior_rows_3d_listener_id), 2).alias("prior_rows_3d_vs_prior_rows_3d_listener_id_avg"),
        F.sum("prior_rows_3d").over(w_all_by_prior_rows_3d_running).alias("running_prior_rows_3d"),
        F.sum("prior_rows_3d").over(w_all_by_prior_rows_3d_prior_3).alias("prior_rows_3d_sum_3_prior"),
    )
)

result = (
    sequenced
    .select(
        "id",
        "listener_id",
        "played_at",
        "prior_rows_3d",
        "listener_interests",
        "prior_rows_3d_percent_rank",
        "prior_rows_3d_vs_prior_rows_3d_listener_id_avg",
        "running_prior_rows_3d",
    )
    .orderBy(F.col("played_at").asc_nulls_last())
)
