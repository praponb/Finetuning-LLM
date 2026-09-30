from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_source_is_skipped_by_ms_play = Window.partitionBy("source", "is_skipped").orderBy(F.col("ms_played").desc())
w_device_len_by_device_len_tie = Window.partitionBy("device_len").orderBy("device_len")
w_device_len_by_device_len_ful = Window.partitionBy("device_len").orderBy("device_len", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_device_len_by_device_len_pri = Window.partitionBy("device_len").orderBy("device_len", "id").rowsBetween(-3, -1)
w_source_device = Window.partitionBy("source", "device")

valid_plays = (
    df_plays
    .filter(F.col("ms_played").between(72464, 236512))
    .select(
        "id",
        "listener_id",
        "track_id",
        "ms_played",
        "device",
        "is_skipped",
        "source",
        F.length(F.trim(F.col("device"))).alias("device_len"),
    )
)

with_windows = (
    valid_plays
    .select(
        "*",
        F.dense_rank().over(w_source_is_skipped_by_ms_play).alias("ms_played_dense_rank"),
        F.rank().over(w_device_len_by_device_len_tie).alias("device_len_rank"),
        F.first("ms_played").over(w_device_len_by_device_len_ful).alias("first_ms_played"),
        F.sum("device_len").over(w_device_len_by_device_len_pri).alias("device_len_sum_3_prior"),
        F.round(F.col("device_len") / F.nullif(F.sum("device_len").over(w_source_device), F.lit(0)), 4).alias("device_len_share_of_source_device"),
        F.size(F.collect_set("track_id").over(w_source_device)).alias("distinct_track_id_in_source_device"),
    )
)

result = (
    with_windows
    .select(
        "id",
        "device_len_sum_3_prior",
        "device_len",
        "first_ms_played",
        "distinct_track_id_in_source_device",
    )
    .unpivot(
        ids=["id"],
        values=["device_len_sum_3_prior", "device_len", "first_ms_played", "distinct_track_id_in_source_device"],
        variableColumnName="metric_name",
        valueColumnName="reading",
    )
    .filter(F.col("reading").isNotNull())
)
