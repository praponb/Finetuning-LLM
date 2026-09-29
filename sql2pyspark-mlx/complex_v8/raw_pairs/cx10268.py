from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_dedup = Window.partitionBy("source_group").orderBy("played_at", "id")

recent = (
    df_plays
    .filter(F.col("source") != "radio")
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "device",
        "is_skipped",
        "source",
        F.hour(F.col("played_at")).alias("played_at_hour"),
        F.lower(F.regexp_replace(F.trim(F.col("device")), " ", "_")).alias("device_slug"),
        F.when(F.col("source").isin("radio", "playlist"), "group_a").when(F.col("source") == "album", "group_b").otherwise("other").alias("source_group"),
    )
)

with_details = (
    recent.alias("s")
    .join(df_listeners.alias("l"), F.col("l.id") == F.col("s.listener_id"), "inner")
    .select(
        "s.id",
        "s.listener_id",
        "s.track_id",
        "s.played_at",
        "s.device",
        "s.is_skipped",
        "s.source",
        "s.device_slug",
        "s.source_group",
        F.col("l.interests").alias("listener_interests"),
        F.col("l.plan").alias("listener_plan"),
        F.col("l.username").alias("listener_username"),
    )
)

unique_rows = (
    with_details
    .withColumn("row_num", F.row_number().over(w_dedup))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

with_details_2 = (
    unique_rows.alias("s")
    .join(df_listeners.alias("l"), F.col("l.id") == F.col("s.listener_id"), "left")
    .select(
        "s.id",
        "s.listener_id",
        "s.track_id",
        "s.played_at",
        "s.device",
        "s.is_skipped",
        "s.device_slug",
        "s.source_group",
        "s.listener_interests",
        "s.listener_plan",
        "s.listener_username",
        F.col("l.birth_year").alias("listener_birth_year"),
    )
)

result = (
    with_details_2
    .groupingSets(
        [["listener_id", "track_id", "listener_plan"], ["listener_id"], []],
        "listener_id", "track_id", "listener_plan",
    )
    .agg(
        F.count("*").alias("row_count"),
        F.min("listener_birth_year").alias("min_listener_birth_year"),
        F.countDistinct("device").alias("distinct_devices"),
    )
)
