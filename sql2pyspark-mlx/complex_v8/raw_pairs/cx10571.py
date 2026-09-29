from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_listener_id_is_skipped_by_ms = Window.partitionBy("listener_id", "is_skipped").orderBy("ms_played", "id").rowsBetween(-2, Window.currentRow)
w_played_at_month = Window.partitionBy(F.month("played_at"))

cleaned = (
    df_plays
    .filter((F.col("is_skipped")) & (F.col("ms_played") <= 333795))
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "ms_played",
        "device",
        "is_skipped",
        "source",
        F.dayofweek(F.col("played_at")).alias("played_at_dayofweek"),
        F.when(F.col("is_skipped"), "yes").otherwise("no").alias("is_skipped_label"),
        F.when(F.col("device").isin("phone", "speaker"), "group_a").when(F.col("device") == "car", "group_b").otherwise("other").alias("device_group"),
    )
)

scored = (
    cleaned
    .select(
        "*",
        F.concat(F.col("device"), F.lit("#"), F.col("played_at_dayofweek").cast("string")).alias("device_ref"),
        F.concat_ws(": ", F.col("source"), F.col("device")).alias("source_device_label"),
        F.datediff(F.lit("2025-06-30").cast("date"), F.col("played_at")).alias("days_since_played_at"),
    )
)

metrics = (
    scored
    .withColumn("played_at_dayofweek_avg_3", F.round(F.avg("played_at_dayofweek").over(w_listener_id_is_skipped_by_ms), 2))
    .withColumn("distinct_device_in_played_at_month", F.size(F.collect_set("device").over(w_played_at_month)))
)

result = (
    metrics
    .cube("source", "distinct_device_in_played_at_month", "device_group")
    .agg(
        F.grouping_id().alias("gid"),
        F.sum("days_since_played_at").alias("total_days_since_played_at"),
        F.max("played_at").alias("last_played_at"),
    )
    .select(
        F.coalesce(F.col("source"), F.lit("ALL")).alias("source"),
        "distinct_device_in_played_at_month",
        F.coalesce(F.col("device_group"), F.lit("ALL")).alias("device_group"),
        "gid",
        "total_days_since_played_at",
        "last_played_at",
    )
    .orderBy(
        F.col("source").asc_nulls_last(),
        F.col("distinct_device_in_played_at_month").asc_nulls_last(),
        F.col("device_group").asc_nulls_last(),
    )
)
