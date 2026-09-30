from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_played_at_month = Window.orderBy("played_at_month", "id")
w_device_or_unknown = Window.partitionBy("device_or_unknown")
w_device_or_unknown_played_at = Window.partitionBy("device_or_unknown", "played_at_month_decile").orderBy(F.col("played_at_month_decile").desc())
w_device_or_unknown_played_at_2 = Window.partitionBy("device_or_unknown", "played_at_month_decile").orderBy(F.col("played_at_month_decile").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

prepared_plays = (
    df_plays
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "device",
        "is_skipped",
        F.date_trunc("month", F.col("played_at")).alias("played_at_month"),
        F.when(F.col("source").isin("search", "radio"), "group_a").when(F.col("source") == "playlist", "group_b").otherwise("other").alias("source_group"),
        F.coalesce(F.col("device"), F.lit("UNKNOWN")).alias("device_or_unknown"),
    )
)

ranked = (
    prepared_plays
    .select(
        "id",
        "listener_id",
        "is_skipped",
        "played_at_month",
        "source_group",
        "device_or_unknown",
        F.ntile(10).over(w_all_by_played_at_month).alias("played_at_month_decile"),
    )
)

ranked_2 = (
    ranked
    .select(
        "*",
        F.round(F.avg("played_at_month_decile").over(w_device_or_unknown), 2).alias("device_or_unknown_avg_played_at_month_decile"),
        F.round(F.cume_dist().over(w_device_or_unknown_played_at), 3).alias("played_at_month_decile_cume_dist"),
        F.last("played_at_month", ignorenulls=True).over(w_device_or_unknown_played_at_2).alias("last_known_played_at_month"),
    )
)

tagged = (
    ranked_2
    .select(
        "id",
        "is_skipped",
        "source_group",
        "device_or_unknown",
        "played_at_month_decile",
        "device_or_unknown_avg_played_at_month_decile",
        "played_at_month_decile_cume_dist",
        "last_known_played_at_month",
        F.to_date(F.col("played_at_month")).alias("played_at_month_date"),
        F.coalesce(F.col("device_or_unknown"), F.lit("UNKNOWN")).alias("device_or_unknown_or_unknown"),
    )
)

result = (
    tagged
    .groupBy("played_at_month_decile")
    .pivot("source_group", ["other", "group_b", "group_a"])
    .agg(F.max("device_or_unknown_avg_played_at_month_decile"))
)
