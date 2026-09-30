from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_listener_id = Window.partitionBy("listener_id")
w_prior_rows_3d_by_played_at_h = Window.partitionBy("prior_rows_3d").orderBy("played_at_hour")
w_listener_id_prior_rows_3d_by = Window.partitionBy("listener_id", "prior_rows_3d").orderBy(F.col("prior_rows_3d").desc())

joined_ranges = (
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

derived = (
    joined_ranges
    .select(
        "id",
        "listener_id",
        "prior_rows_3d",
        (F.col("prior_rows_3d") - F.col("prior_ms_played_3d")).alias("prior_rows_3d_minus_prior_ms_played_3d"),
        F.date_trunc("hour", F.col("played_at")).alias("played_at_hour"),
    )
)

ranked = (
    derived
    .select(
        "id",
        "listener_id",
        "played_at_hour",
        F.min("prior_rows_3d").over(w_listener_id).alias("listener_id_min_prior_rows_3d"),
        F.rank().over(w_prior_rows_3d_by_played_at_h).alias("played_at_hour_rank"),
        F.rank().over(w_listener_id_prior_rows_3d_by).alias("prior_rows_3d_rank"),
    )
)

result = (
    ranked
    .cube("prior_rows_3d_rank", "played_at_hour_rank")
    .agg(
        F.percentile_approx("prior_rows_3d_rank", 0.5).alias("median_prior_rows_3d_rank"),
        F.sum("prior_rows_3d_rank").alias("total_prior_rows_3d_rank"),
        F.percentile_approx("played_at_hour_rank", 0.5).alias("median_played_at_hour_rank"),
    )
    .orderBy(
        F.col("prior_rows_3d_rank").asc_nulls_last(),
        F.col("played_at_hour_rank").asc_nulls_last(),
    )
)
