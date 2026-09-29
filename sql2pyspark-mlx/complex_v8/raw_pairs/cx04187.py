from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_device_len = Window.partitionBy("device_len")
w_listener_id_track_id_by_devi = Window.partitionBy("listener_id", "track_id").orderBy(F.col("device_len_2").desc(), "id")
w_listener_id_track_id_by_devi_2 = Window.partitionBy("listener_id", "track_id").orderBy(F.col("device_len_2").desc(), "id").rowsBetween(-2, Window.currentRow)
w_listener_id_track_id_by_devi_3 = Window.partitionBy("listener_id", "track_id").orderBy(F.col("device_len_2").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_listener_id_device_len_2 = Window.partitionBy("listener_id", "device_len_2")
w_all_by_listener_birth_year = Window.orderBy(F.col("listener_birth_year").desc(), "id")

prepared = (
    df_plays
    .filter((~F.col("is_skipped")) & (F.col("track_id") >= 19))
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "device",
        "is_skipped",
        "source",
    )
)

with_parent_listener = (
    prepared.alias("s")
    .join(df_listeners.alias("l"), F.col("l.id") == F.col("s.listener_id"), "left")
    .join(df_tracks.alias("t"), F.col("t.id") == F.col("s.track_id"), "left")
    .select(
        "s.id",
        "s.listener_id",
        "s.track_id",
        "s.device",
        "s.is_skipped",
        "s.source",
        F.col("l.birth_year").alias("listener_birth_year"),
        F.col("t.moods").alias("track_moods"),
    )
)

scored = (
    with_parent_listener
    .select(
        "*",
        F.length(F.trim(F.col("device"))).alias("device_len"),
        F.length(F.trim(F.col("device"))).alias("device_len_2"),
    )
)

sequenced = (
    scored
    .select(
        "*",
        F.round(F.col("device_len_2") - F.avg("device_len_2").over(w_device_len), 2).alias("device_len_2_vs_device_len_avg"),
        F.lead("device_len", 3).over(w_listener_id_track_id_by_devi).alias("next3_device_len"),
        F.round(F.avg("device_len").over(w_listener_id_track_id_by_devi_2), 2).alias("device_len_avg_3"),
        F.first("listener_birth_year", ignorenulls=True).over(w_listener_id_track_id_by_devi_3).alias("first_known_listener_birth_year"),
        F.count(F.lit(1)).over(w_device_len).alias("rows_in_device_len"),
        F.lag("device_len_2").over(w_listener_id_track_id_by_devi).alias("prev_device_len_2"),
    )
)

metrics = (
    sequenced
    .select(
        "id",
        "listener_id",
        "device",
        "is_skipped",
        "device_len",
        "device_len_2_vs_device_len_avg",
        "next3_device_len",
        "rows_in_device_len",
        "prev_device_len_2",
        F.count(F.lit(1)).over(w_listener_id_device_len_2).alias("rows_in_listener_id_device_len_2"),
        (F.col("rows_in_device_len") - F.lag("rows_in_device_len").over(w_all_by_listener_birth_year)).alias("rows_in_device_len_change"),
        (F.col("next3_device_len") - F.lag("next3_device_len").over(w_all_by_listener_birth_year)).alias("next3_device_len_change"),
    )
)

result = (
    metrics
    .select(
        "id",
        "listener_id",
        "device",
        "device_len",
        "device_len_2_vs_device_len_avg",
        "rows_in_device_len",
        "prev_device_len_2",
        "rows_in_listener_id_device_len_2",
        "next3_device_len_change",
        F.coalesce(F.col("device"), F.lit("UNKNOWN")).alias("device_or_unknown"),
        F.when(F.col("device").isin("desktop", "phone"), "group_a").when(F.col("device") == "car", "group_b").otherwise("other").alias("device_group"),
    )
    .orderBy("next3_device_len_change", F.col("device_group").desc(), "id")
    .limit(20)
)
