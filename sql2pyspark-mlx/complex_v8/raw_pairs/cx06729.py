from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_latest = Window.partitionBy("listener_id", "source").orderBy(F.col("ms_played_distance").desc(), F.col("id").desc())
w_is_skipped_by_ms_played_dist = Window.partitionBy("is_skipped").orderBy("ms_played_distance", "id")
w_listener_id_source = Window.partitionBy("listener_id", "source")

base = (
    df_plays
    .filter((F.col("device") == "phone") & (F.col("track_id") >= 24))
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "ms_played",
        "is_skipped",
        "source",
        F.abs(F.col("ms_played") - 176302).alias("ms_played_distance"),
    )
)

without_matches = (
    base.alias("s")
    .join(df_plays.alias("p"), (F.col("p.listener_id") == F.col("s.listener_id")) & (F.col("p.device").isin("phone", "speaker", "car")), "left_semi")
)

unique_rows = (
    without_matches
    .withColumn("row_num", F.row_number().over(w_latest))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

windowed = (
    unique_rows
    .select(
        "id",
        "listener_id",
        "ms_played",
        "is_skipped",
        "source",
        F.ntile(4).over(w_is_skipped_by_ms_played_dist).alias("ms_played_distance_quartile"),
        F.count(F.lit(1)).over(w_listener_id_source).alias("rows_in_listener_id_source"),
    )
)

without_matches_2 = (
    windowed.alias("s")
    .join(df_plays.alias("p"), (F.col("p.listener_id") == F.col("s.listener_id")) & (F.to_date(F.col("p.played_at")) < F.lit("2025-02-12").cast("date")) & (F.col("p.device") != "phone"), "left_anti")
)

with_details_listener = (
    without_matches_2.alias("s")
    .join(df_listeners.alias("l"), F.col("l.id") == F.col("s.listener_id"), "inner")
    .select(
        "s.id",
        "s.listener_id",
        "s.ms_played",
        "s.is_skipped",
        "s.source",
        "s.ms_played_distance_quartile",
        "s.rows_in_listener_id_source",
        F.col("l.interests").alias("listener_interests"),
        F.col("l.plan").alias("listener_plan"),
        F.col("l.username").alias("listener_username"),
    )
)

result = (
    with_details_listener
    .filter(F.col("rows_in_listener_id_source") < 12)
    .select(
        "id",
        "listener_id",
        "ms_played",
        "is_skipped",
        "source",
        "ms_played_distance_quartile",
        "listener_plan",
        "listener_username",
    )
    .orderBy(F.col("id").asc())
)
