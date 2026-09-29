from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_prior_rows_12h_full = Window.orderBy(F.col("prior_rows_12h").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_prior_rows_12h = Window.orderBy(F.col("prior_rows_12h").desc(), "id")
w_listener_id = Window.partitionBy("listener_id")
w_all_by_prior_rows_12h_runnin = Window.orderBy(F.col("prior_rows_12h").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

matched = (
    df_plays.alias("a")
    .join(
        df_plays.alias("b"),
        (F.col("b.listener_id") == F.col("a.listener_id"))
        & (F.col("b.played_at") < F.col("a.played_at"))
        & (F.col("b.played_at") >= F.col("a.played_at") - F.expr("INTERVAL 12 HOURS")),
        "left",
    )
    .groupBy("a.id", "a.listener_id", "a.played_at")
    .agg(
        F.count("b.id").alias("prior_rows_12h"),
        F.sum("b.ms_played").alias("prior_ms_played_12h"),
    )
)

with_windows = (
    matched
    .select(
        "*",
        F.first("prior_ms_played_12h").over(w_all_by_prior_rows_12h_full).alias("first_prior_ms_played_12h"),
        F.rank().over(w_all_by_prior_rows_12h).alias("prior_rows_12h_rank"),
        F.round(F.avg("prior_rows_12h").over(w_listener_id), 2).alias("listener_id_avg_prior_rows_12h"),
        F.last("played_at", ignorenulls=True).over(w_all_by_prior_rows_12h_runnin).alias("last_known_played_at"),
    )
)

result = (
    with_windows
    .select(
        "id",
        "listener_id",
        "played_at",
        "prior_rows_12h",
        "prior_ms_played_12h",
        "first_prior_ms_played_12h",
        "prior_rows_12h_rank",
        "listener_id_avg_prior_rows_12h",
        F.round(F.col("prior_ms_played_12h") / F.nullif(F.col("prior_rows_12h_rank"), F.lit(0)), 3).alias("prior_ms_played_12h_per_prior_rows_12h_rank"),
    )
    .orderBy(F.col("prior_rows_12h").desc_nulls_first(), "listener_id")
)
