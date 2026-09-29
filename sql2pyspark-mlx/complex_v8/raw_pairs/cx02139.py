from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_listener_id_is_skipped = Window.partitionBy("listener_id", "is_skipped")
w_is_skipped_by_ms_played_prio = Window.partitionBy("is_skipped").orderBy("ms_played", "id").rowsBetween(-3, -1)
w_is_skipped_by_ms_played_runn = Window.partitionBy("is_skipped").orderBy("ms_played", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_is_skipped_by_ms_played = Window.partitionBy("is_skipped").orderBy("ms_played", "id")

recent = (
    df_plays
    .filter((F.col("ms_played") < 326250) & (F.col("is_skipped")))
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "ms_played",
        "device",
        "is_skipped",
        "source",
        F.length(F.trim(F.col("device"))).alias("device_len"),
        F.hour(F.col("played_at")).alias("played_at_hour"),
        F.date_trunc("week", F.col("played_at")).alias("played_at_week"),
    )
)

windowed = (
    recent
    .select(
        "id",
        "listener_id",
        "played_at",
        "device",
        "is_skipped",
        "source",
        "device_len",
        "played_at_hour",
        F.round(F.col("device_len") / F.nullif(F.sum("device_len").over(w_listener_id_is_skipped), F.lit(0)), 4).alias("device_len_share_of_listener_id_is_skipped"),
        F.round(F.avg("ms_played").over(w_is_skipped_by_ms_played_prio), 2).alias("ms_played_avg_3_prior"),
        F.max("ms_played").over(w_is_skipped_by_ms_played_runn).alias("max_ms_played_so_far"),
        F.rank().over(w_is_skipped_by_ms_played).alias("ms_played_rank"),
    )
)

derived = (
    windowed
    .filter(~F.col("source").isin("album", "radio"))
    .select(
        "*",
        (F.col("played_at_hour") + F.col("device_len")).alias("played_at_hour_plus_device_len"),
        F.concat_ws(" / ", F.col("device"), F.col("source")).alias("device_source_label"),
    )
)

result = (
    derived
    .filter(F.col("max_ms_played_so_far").between(604250, 2801500))
    .select(
        "id",
        "listener_id",
        "played_at",
        "device",
        "is_skipped",
        "device_len",
        "device_len_share_of_listener_id_is_skipped",
        "played_at_hour_plus_device_len",
        "device_source_label",
        F.lower(F.regexp_replace(F.trim(F.col("source")), " ", "_")).alias("source_slug"),
        (F.col("played_at") + F.expr("INTERVAL 7 DAYS")).alias("played_at_plus_7d"),
    )
)
