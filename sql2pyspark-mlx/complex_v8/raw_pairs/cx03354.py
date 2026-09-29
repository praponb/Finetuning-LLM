from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_played_at = Window.orderBy(F.col("played_at").desc(), "id")
w_device = Window.partitionBy("device")

valid = (
    df_plays
    .filter((F.col("is_skipped")) | (F.col("listener_id").isNotNull()))
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "ms_played",
        "device",
        "is_skipped",
    )
)

qualified = (
    valid.alias("s")
    .join(df_plays.alias("p"), (F.col("p.listener_id") == F.col("s.listener_id")) & (F.col("p.track_id").isNotNull()) & (F.col("p.ms_played") < 261579), "left_semi")
    .filter(F.to_date(F.col("played_at")) < F.lit("2025-03-08").cast("date"))
)

windowed = (
    qualified
    .select(
        "*",
        F.lead("ms_played", 3, 0).over(w_all_by_played_at).alias("next3_ms_played"),
        F.count(F.lit(1)).over(w_device).alias("rows_in_device"),
    )
)

result = (
    windowed
    .groupingSets(
        [["is_skipped", "listener_id"], ["is_skipped"], ["listener_id"]],
        "is_skipped", "listener_id",
    )
    .agg(
        F.round(F.sum("rows_in_device") / F.nullif(F.sum("next3_ms_played"), F.lit(0)), 3).alias("rows_in_device_to_next3_ms_played_ratio"),
        F.max("played_at").alias("last_played_at"),
        F.count(F.when(~F.col("is_skipped"), 1)).alias("n_is_skipped_false"),
    )
    .orderBy(
        F.col("is_skipped").asc_nulls_last(),
        F.col("listener_id").asc_nulls_last(),
    )
)
