from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_listener_id_by_prior_ms_play = Window.partitionBy("listener_id").orderBy(F.col("prior_ms_played_7d_filled").desc(), "id")
w_prior_rows_7d_listener_id = Window.partitionBy("prior_rows_7d", "listener_id")
w_listener_id_prior_rows_7d_by = Window.partitionBy("listener_id", "prior_rows_7d").orderBy(F.col("prior_ms_played_7d_filled").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_listener_id_by_prior_ms_play_2 = Window.partitionBy("listener_id").orderBy(F.col("prior_ms_played_7d_filled").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_listener_id_prior_rows_7d_by_2 = Window.partitionBy("listener_id", "prior_rows_7d").orderBy(F.col("prior_ms_played_7d_filled").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_listener_id_by_prior_ms_play_3 = Window.partitionBy("listener_id").orderBy(F.col("prior_ms_played_7d_filled").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

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

tagged = (
    joined_ranges
    .select(
        "id",
        "listener_id",
        "prior_rows_7d",
        F.least(F.col("prior_rows_7d"), F.col("prior_ms_played_7d")).alias("least_prior_rows_7d_prior_ms_played_7d"),
        F.coalesce(F.col("prior_ms_played_7d"), F.lit(0)).alias("prior_ms_played_7d_filled"),
    )
)

ranked = (
    tagged
    .select(
        "*",
        F.lag("prior_ms_played_7d_filled").over(w_listener_id_by_prior_ms_play).alias("prev_prior_ms_played_7d_filled"),
        F.count(F.lit(1)).over(w_listener_id_prior_rows_7d_by).alias("running_count"),
        F.nth_value("prior_rows_7d", 2).over(w_listener_id_by_prior_ms_play_2).alias("second_prior_rows_7d"),
        F.first("prior_rows_7d").over(w_listener_id_prior_rows_7d_by_2).alias("first_prior_rows_7d"),
        F.max("prior_ms_played_7d_filled").over(w_listener_id_by_prior_ms_play_3).alias("max_prior_ms_played_7d_filled_so_far"),
        (F.col("prior_ms_played_7d_filled") - F.lag("prior_ms_played_7d_filled").over(w_listener_id_by_prior_ms_play)).alias("prior_ms_played_7d_filled_change"),
    )
)

derived = (
    ranked
    .select(
        "*",
        F.round(F.col("first_prior_rows_7d") / F.nullif(F.col("prior_rows_7d"), F.lit(0)), 3).alias("first_prior_rows_7d_per_prior_rows_7d"),
        F.least(F.col("running_count"), F.col("second_prior_rows_7d")).alias("least_running_count_second_prior_rows_7d"),
        F.round(F.col("running_count") / F.nullif(F.col("second_prior_rows_7d"), F.lit(0)), 3).alias("running_count_per_second_prior_rows_7d"),
    )
)

result = (
    derived
    .filter(F.col("max_prior_ms_played_7d_filled_so_far") >= 5000000)
    .select(
        "id",
        "listener_id",
        "prior_rows_7d",
        "least_prior_rows_7d_prior_ms_played_7d",
        "prior_ms_played_7d_filled",
        "prev_prior_ms_played_7d_filled",
        "running_count",
        "second_prior_rows_7d",
        "first_prior_rows_7d",
        "max_prior_ms_played_7d_filled_so_far",
        "prior_ms_played_7d_filled_change",
        "least_running_count_second_prior_rows_7d",
        "running_count_per_second_prior_rows_7d",
        F.round(F.col("first_prior_rows_7d") / F.nullif(F.col("running_count"), F.lit(0)), 3).alias("first_prior_rows_7d_per_running_count"),
    )
)
