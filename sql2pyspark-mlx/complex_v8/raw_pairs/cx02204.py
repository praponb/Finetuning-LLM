from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_device = Window.partitionBy("device").orderBy("played_at", "id")
w_device_running = Window.partitionBy("device").orderBy("played_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

prepared = (
    df_plays
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "ms_played",
        "device",
    )
)

with_prev = prepared.withColumn("prev_played_at", F.lag("played_at").over(w_device))

flagged = (
    with_prev
    .withColumn("new_session_flag", F.when(F.col("prev_played_at").isNull() | ((F.unix_timestamp("played_at") - F.unix_timestamp("prev_played_at")) / 60 > 30), 1).otherwise(0))
)

sessionized = flagged.withColumn("session_id", F.sum("new_session_flag").over(w_device_running))

result = (
    sessionized
    .groupingSets(
        [["new_session_flag", "device"], ["new_session_flag"], []],
        "new_session_flag", "device",
    )
    .agg(
        F.array_sort(F.collect_set("device")).alias("device_list"),
        F.round(F.sum("ms_played") / F.nullif(F.sum("session_id"), F.lit(0)), 3).alias("ms_played_to_session_id_ratio"),
    )
    .select(
        "new_session_flag",
        F.coalesce(F.col("device"), F.lit("ALL")).alias("device"),
        "device_list",
        "ms_played_to_session_id_ratio",
    )
    .orderBy(
        F.col("new_session_flag").asc_nulls_last(),
        F.col("device").asc_nulls_last(),
    )
)
