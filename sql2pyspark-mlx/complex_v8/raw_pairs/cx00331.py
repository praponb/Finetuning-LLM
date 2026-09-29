from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_source_listener_id_by_played = Window.partitionBy("source", "listener_id").orderBy(F.col("played_at").desc(), "id")
w_listener_id_by_played_at = Window.partitionBy("listener_id").orderBy("played_at", "id")
w_listener_id = Window.partitionBy("listener_id")
w_listener_id_by_played_at_tie = Window.partitionBy("listener_id").orderBy("played_at")

valid = (
    df_plays
    .filter(F.col("ms_played").between(97825, 257789))
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "ms_played",
        "device",
        "is_skipped",
        "source",
        F.when(F.col("ms_played") >= 245815, "large").when(F.col("ms_played") >= 99376, "medium").otherwise("small").alias("ms_played_tier"),
    )
)

windowed = (
    valid
    .select(
        "id",
        "listener_id",
        "played_at",
        "ms_played",
        "is_skipped",
        "source",
        F.row_number().over(w_source_listener_id_by_played).alias("rn"),
        F.ntile(10).over(w_listener_id_by_played_at).alias("played_at_decile"),
        F.round(F.col("ms_played") / F.nullif(F.sum("ms_played").over(w_listener_id), F.lit(0)), 4).alias("ms_played_share_of_listener_id"),
        F.dense_rank().over(w_listener_id_by_played_at_tie).alias("played_at_dense_rank"),
    )
)

with_details_listener = (
    windowed.alias("s")
    .join(df_listeners.alias("l"), F.col("l.id") == F.col("s.listener_id"), "left")
    .select(
        "s.id",
        "s.listener_id",
        "s.played_at",
        "s.ms_played",
        "s.source",
        "s.rn",
        "s.played_at_decile",
        "s.ms_played_share_of_listener_id",
        "s.played_at_dense_rank",
        F.col("l.username").alias("listener_username"),
        F.col("l.country").alias("listener_country"),
        F.col("l.plan").alias("listener_plan"),
    )
)

qualified = (
    with_details_listener.alias("s")
    .join(df_listeners.alias("l"), (F.col("l.id") == F.col("s.listener_id")) & (F.col("l.country").isNotNull()) & (F.lower(F.col("l.username")).like("%a.%")), "left_semi")
)

result = (
    qualified
    .select(
        "id",
        "listener_id",
        "played_at",
        "source",
        "rn",
        "ms_played_share_of_listener_id",
        "played_at_dense_rank",
        "listener_country",
        "listener_plan",
    )
)
