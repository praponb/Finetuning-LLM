from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_played_at_dayofweek_r = Window.orderBy("played_at_dayofweek", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_played_at_dayofweek = Window.orderBy("played_at_dayofweek", "id")

prepared = (
    df_plays
    .filter(F.hour(F.col("played_at")).between(6, 9))
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "ms_played",
        "device",
        "is_skipped",
        "source",
    )
)

annotated = (
    prepared
    .filter(F.col("track_id") <= 20)
    .select(
        "*",
        F.when(F.col("device").isin("desktop", "phone"), "group_a").when(F.col("device") == "speaker", "group_b").otherwise("other").alias("device_group"),
        F.concat(F.col("device"), F.lit("#"), F.col("listener_id").cast("string")).alias("device_ref"),
        F.dayofweek(F.col("played_at")).alias("played_at_dayofweek"),
    )
)

windowed = (
    annotated
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "ms_played",
        "device",
        "source",
        "device_group",
        "played_at_dayofweek",
        F.last("ms_played", ignorenulls=True).over(w_all_by_played_at_dayofweek_r).alias("last_known_ms_played"),
        F.ntile(3).over(w_all_by_played_at_dayofweek).alias("played_at_dayofweek_tercile"),
    )
)

result = (
    windowed
    .groupingSets(
        [["device_group", "track_id", "listener_id"], ["device_group"], ["track_id"]],
        "device_group", "track_id", "listener_id",
    )
    .agg(
        F.grouping_id().alias("grouping_level"),
        F.sum("ms_played").alias("total_ms_played"),
    )
    .orderBy(
        F.col("device_group").asc_nulls_last(),
        F.col("track_id").asc_nulls_last(),
        F.col("listener_id").asc_nulls_last(),
    )
)
