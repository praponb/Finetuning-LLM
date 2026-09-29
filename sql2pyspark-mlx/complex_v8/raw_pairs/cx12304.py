from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_dedup = Window.partitionBy("device").orderBy("played_at", "id")

base = (
    df_plays
    .filter(F.col("device") == "desktop")
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "device",
        "is_skipped",
    )
)

latest = (
    base
    .withColumn("row_num", F.row_number().over(w_dedup))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

with_details = (
    latest.alias("s")
    .join(df_listeners.alias("l"), F.col("l.id") == F.col("s.listener_id"), "left")
    .select(
        "s.*",
        F.col("l.country").alias("listener_country"),
        F.col("l.plan").alias("listener_plan"),
    )
)

qualified = (
    with_details.alias("s")
    .join(df_plays.alias("p"), (F.col("p.listener_id") == F.col("s.listener_id")) & (F.col("p.device") != "car") & (~F.col("p.is_skipped")), "left_semi")
)

result = (
    qualified
    .groupingSets(
        [["is_skipped", "listener_id"], ["is_skipped"], ["listener_id"], []],
        "is_skipped", "listener_id",
    )
    .agg(
        F.countDistinct("listener_id").alias("distinct_listener_ids"),
        F.countDistinct("track_id").alias("distinct_track_ids"),
        F.count(F.when(F.col("device") == "tv", 1)).alias("n_device_tv"),
    )
)
