from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_latest = Window.partitionBy("device", "track_id").orderBy(F.col("played_at").desc(), F.col("id").desc())
w_listener_id_track_id_by_play = Window.partitionBy("listener_id", "track_id").orderBy("played_at_hour", "id")
w_listener_id_track_id_by_play_2 = Window.partitionBy("listener_id", "track_id").orderBy("played_at_hour")

base = (
    df_plays
    .filter(F.col("device").isin("desktop", "speaker", "tv"))
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "ms_played",
        "device",
        "is_skipped",
        F.hour(F.col("played_at")).alias("played_at_hour"),
        (F.col("played_at") + F.expr("INTERVAL 1 DAYS")).alias("played_at_plus_1d"),
    )
)

with_activity = (
    base
    .crossJoin(base.agg(F.max("played_at_hour").alias("overall_max_played_at_hour")))
    .filter(F.col("played_at_hour") < F.col("overall_max_played_at_hour"))
    .drop("overall_max_played_at_hour")
)

with_activity_2 = (
    with_activity.alias("s")
    .join(df_plays.alias("p"), (F.col("p.track_id") == F.col("s.track_id")) & (F.col("p.source") != "album"), "left_semi")
)

deduped = (
    with_activity_2
    .withColumn("row_num", F.row_number().over(w_latest))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

labeled = (
    deduped
    .filter(F.col("listener_id").isNotNull())
    .select(
        "*",
        (F.col("played_at") + F.expr("INTERVAL 7 DAYS")).alias("played_at_plus_7d"),
        F.when(F.col("device").isin("phone", "car"), "group_a").when(F.col("device") == "tv", "group_b").otherwise("other").alias("device_group"),
        F.round(F.col("ms_played") / F.nullif(F.col("played_at_hour"), F.lit(0)), 3).alias("ms_played_per_played_at_hour"),
    )
)

ranked = (
    labeled
    .select(
        "id",
        "ms_played",
        "device",
        "is_skipped",
        "played_at_plus_1d",
        "played_at_plus_7d",
        F.row_number().over(w_listener_id_track_id_by_play).alias("rn"),
        F.round(F.cume_dist().over(w_listener_id_track_id_by_play_2), 3).alias("played_at_hour_cume_dist"),
    )
)

result = (
    ranked
    .select("id", "rn", "ms_played")
    .unpivot(
        ids=["id"],
        values=["rn", "ms_played"],
        variableColumnName="field",
        valueColumnName="value",
    )
    .filter(F.col("value").isNotNull())
)
