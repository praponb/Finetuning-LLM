from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_latest = Window.partitionBy("source", "device").orderBy("played_at", "id")
w_track_id_by_played_at_full = Window.partitionBy("track_id").orderBy("played_at", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

scoped = (
    df_plays
    .filter(F.col("listener_id") <= 11)
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "device",
        "source",
        F.date_format(F.col("played_at"), "yyyy-MM").alias("played_at_label"),
        F.coalesce(F.col("device"), F.lit("UNKNOWN")).alias("device_or_unknown"),
    )
)

deduped = (
    scoped
    .withColumn("rn", F.row_number().over(w_latest))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

windowed = (
    deduped
    .select(
        "id",
        "listener_id",
        "track_id",
        "device",
        "played_at_label",
        "device_or_unknown",
        F.last("played_at").over(w_track_id_by_played_at_full).alias("last_played_at"),
        F.nth_value("played_at", 2).over(w_track_id_by_played_at_full).alias("second_played_at"),
    )
)

result = (
    windowed
    .cube("listener_id", "device")
    .agg(
        F.sum(F.when(F.col("device").isin("tv", "phone", "car"), 1).otherwise(0)).alias("n_device_in_tv_phone_car"),
        F.count("*").alias("cnt"),
    )
    .orderBy(F.col("listener_id").asc_nulls_last(), F.col("device").asc_nulls_last())
)
