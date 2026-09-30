from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_played_at_running = Window.orderBy("played_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_listener_id_played_at_to_dat = Window.partitionBy("listener_id", F.to_date("played_at")).orderBy("played_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_listener_id_played_at_to_dat_2 = Window.partitionBy("listener_id", F.to_date("played_at")).orderBy("played_at")

recent_plays = (
    df_plays
    .filter((F.col("track_id").isNotNull()) & (F.col("device").isin("tv", "desktop", "phone")))
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "ms_played",
        "device",
        "is_skipped",
        "source",
        F.concat(F.col("source"), F.lit("#"), F.col("track_id").cast("string")).alias("source_ref"),
        F.when(F.col("is_skipped"), "yes").otherwise("no").alias("is_skipped_label"),
        F.concat(F.col("source"), F.lit("#"), F.col("id").cast("string")).alias("source_ref_2"),
    )
)

sequenced = (
    recent_plays
    .select(
        "*",
        F.sum("ms_played").over(w_all_by_played_at_running).alias("running_ms_played"),
        F.last("ms_played", ignorenulls=True).over(w_all_by_played_at_running).alias("last_known_ms_played"),
        F.last("ms_played", ignorenulls=True).over(w_listener_id_played_at_to_dat).alias("last_known_ms_played_2"),
        F.dense_rank().over(w_listener_id_played_at_to_dat_2).alias("played_at_dense_rank"),
    )
)

result = (
    sequenced
    .select(
        "id",
        "played_at_dense_rank",
        "ms_played",
        "last_known_ms_played",
    )
    .unpivot(
        ids=["id"],
        values=["played_at_dense_rank", "ms_played", "last_known_ms_played"],
        variableColumnName="metric",
        valueColumnName="metric_value",
    )
    .filter(F.col("metric_value").isNotNull())
    .orderBy("id", "metric")
)
