from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_track_id_source = Window.partitionBy("track_id", "source")
w_track_id_listener_id_by_days = Window.partitionBy("track_id", "listener_id").orderBy(F.col("days_since_played_at").desc(), "id")

valid = (
    df_plays
    .filter((F.col("track_id") >= 6) & (F.col("is_skipped")))
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "ms_played",
        "device",
        "source",
        F.hour(F.col("played_at")).alias("played_at_hour"),
        F.datediff(F.lit("2025-06-30").cast("date"), F.col("played_at")).alias("days_since_played_at"),
        F.when(F.col("source").isin("album", "playlist"), "group_a").when(F.col("source") == "search", "group_b").otherwise("other").alias("source_group"),
    )
)

windowed = (
    valid
    .select(
        "*",
        F.round(F.col("played_at_hour") - F.avg("played_at_hour").over(w_track_id_source), 2).alias("played_at_hour_vs_track_id_source_avg"),
        F.count(F.lit(1)).over(w_track_id_source).alias("rows_in_track_id_source"),
        F.row_number().over(w_track_id_listener_id_by_days).alias("seq_no"),
    )
)

scored = (
    windowed
    .select(
        "*",
        F.coalesce(F.col("seq_no"), F.lit(0)).alias("seq_no_filled"),
        F.lower(F.regexp_replace(F.trim(F.col("source_group")), " ", "_")).alias("source_group_slug"),
    )
)

result = (
    scored
    .rollup("source", "played_at_hour", "seq_no")
    .agg(
        F.grouping_id().alias("gid"),
        F.count("*").alias("n_rows"),
        F.max("played_at").alias("last_played_at"),
        F.countDistinct("source_group").alias("distinct_source_groups"),
    )
    .orderBy(
        F.col("source").asc_nulls_last(),
        F.col("played_at_hour").asc_nulls_last(),
        F.col("seq_no").asc_nulls_last(),
    )
)
