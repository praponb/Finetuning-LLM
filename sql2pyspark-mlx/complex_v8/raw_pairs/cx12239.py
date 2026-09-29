from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_track_id_device_by_ms_played = Window.partitionBy("track_id", "device").orderBy("ms_played", "id").rowsBetween(-2, -1)
w_device = Window.partitionBy("device")

recent_plays = (
    df_plays
    .filter(F.col("device").isin("speaker", "phone"))
    .select(
        "id",
        "listener_id",
        "track_id",
        "ms_played",
        "device",
        "is_skipped",
        F.substring(F.trim(F.col("device")), 1, 3).alias("device_prefix"),
        F.concat_ws("-", F.col("source"), F.col("device")).alias("source_device_label"),
        F.lower(F.trim(F.col("source"))).alias("source_clean"),
    )
)

with_windows = (
    recent_plays
    .select(
        "id",
        "listener_id",
        "track_id",
        "ms_played",
        "device",
        "is_skipped",
        "device_prefix",
        "source_device_label",
        "source_clean",
        F.sum("ms_played").over(w_track_id_device_by_ms_played).alias("ms_played_sum_2_prior"),
        F.min("ms_played").over(w_device).alias("device_min_ms_played"),
    )
)

derived = (
    with_windows
    .select(
        "*",
        F.greatest(F.col("device_min_ms_played"), F.col("ms_played_sum_2_prior")).alias("greatest_device_min_ms_played_ms_played_sum_2_pr"),
        (F.col("device_min_ms_played") - F.col("ms_played")).alias("device_min_ms_played_minus_ms_played"),
    )
)

result = (
    derived
    .select(
        "id",
        "ms_played_sum_2_prior",
        "device_min_ms_played",
        "device_min_ms_played_minus_ms_played",
        "ms_played",
    )
    .unpivot(
        ids=["id"],
        values=["ms_played_sum_2_prior", "device_min_ms_played", "device_min_ms_played_minus_ms_played", "ms_played"],
        variableColumnName="measure",
        valueColumnName="amount",
    )
    .filter(F.col("amount").isNotNull())
)
