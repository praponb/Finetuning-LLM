from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_prior_rows_7d_listener_id = Window.partitionBy("prior_rows_7d", "listener_id")

matched = (
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
    matched
    .select(
        "*",
        F.round(F.col("prior_rows_7d") / F.nullif(F.sum("prior_rows_7d").over(w_prior_rows_7d_listener_id), F.lit(0)), 4).alias("prior_rows_7d_share_of_prior_rows_7d_listener_id"),
        F.max("prior_rows_7d").over(w_prior_rows_7d_listener_id).alias("prior_rows_7d_listener_id_max_prior_rows_7d"),
    )
)

stats = (
    with_windows
    .groupBy("prior_rows_7d", "listener_id")
    .agg(
        F.min("prior_rows_7d").alias("min_prior_rows_7d"),
        F.count("*").alias("row_count"),
        F.max("played_at").alias("last_played_at"),
    )
)

filtered = (
    stats.alias("s")
    .join(df_listeners.alias("l"), (F.col("l.id") == F.col("s.listener_id")) & (F.col("l.plan") == "premium"), "left_anti")
)

result = (
    filtered
    .select(
        "prior_rows_7d",
        "listener_id",
        "min_prior_rows_7d",
        "row_count",
        "last_played_at",
        (F.col("row_count") + F.col("min_prior_rows_7d")).alias("row_count_plus_min_prior_rows_7d"),
    )
)
