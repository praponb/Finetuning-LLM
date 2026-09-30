from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_track_id_by_played_at_runnin = Window.partitionBy("track_id").orderBy("played_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_track_id_by_played_at_prior = Window.partitionBy("track_id").orderBy("played_at", "id").rowsBetween(-1, -1)
w_device_played_at_to_date = Window.partitionBy("device", F.to_date("played_at"))
w_track_id_by_played_at = Window.partitionBy("track_id").orderBy("played_at", "id")

recent_plays = (
    df_plays
    .filter(F.col("played_at") < F.lit("2025-02-24 09:00:00").cast("timestamp"))
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "device",
        "is_skipped",
        "source",
        F.when(F.col("is_skipped"), "yes").otherwise("no").alias("is_skipped_label"),
        F.datediff(F.lit("2025-12-31").cast("date"), F.col("played_at")).alias("days_since_played_at"),
    )
)

scored = (
    recent_plays
    .filter(F.col("device") != "tv")
    .select(
        "*",
        F.coalesce(F.col("days_since_played_at"), F.lit(0)).alias("days_since_played_at_filled"),
        F.to_date(F.col("played_at")).alias("played_at_date"),
    )
)

without_matches = (
    scored.alias("s")
    .join(df_plays.alias("p"), (F.col("p.track_id") == F.col("s.track_id")) & (F.col("p.source").isNotNull()) & (F.col("p.listener_id").isNotNull()), "left_anti")
)

ranked = (
    without_matches
    .select(
        "*",
        F.sum("days_since_played_at").over(w_track_id_by_played_at_runnin).alias("running_days_since_played_at"),
        F.max("days_since_played_at").over(w_track_id_by_played_at_prior).alias("days_since_played_at_max_1_prior"),
        F.count(F.lit(1)).over(w_device_played_at_to_date).alias("rows_in_device_played_at_to_date"),
        F.ntile(10).over(w_track_id_by_played_at).alias("played_at_decile"),
        F.round(F.col("days_since_played_at") - F.avg("days_since_played_at").over(w_device_played_at_to_date), 2).alias("days_since_played_at_vs_device_played_at_to_date"),
    )
)

result = (
    ranked
    .filter(F.col("days_since_played_at") <= 385)
    .select(
        "id",
        "listener_id",
        "device",
        "is_skipped_label",
        "days_since_played_at",
        "days_since_played_at_filled",
        "played_at_date",
        "days_since_played_at_max_1_prior",
        "played_at_decile",
        "days_since_played_at_vs_device_played_at_to_date",
        F.when(F.col("is_skipped"), "yes").otherwise("no").alias("is_skipped_label_2"),
    )
)
