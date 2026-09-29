from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_ms_played = Window.orderBy("ms_played", "id")
w_track_id = Window.partitionBy("track_id")

base_plays = (
    df_plays
    .filter((~F.col("is_skipped")) & (F.col("track_id").isNotNull()))
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "ms_played",
        "device",
        "source",
    )
)

windowed = (
    base_plays
    .select(
        "*",
        F.lead("ms_played", 2).over(w_all_by_ms_played).alias("next2_ms_played"),
        F.count(F.lit(1)).over(w_track_id).alias("rows_in_track_id"),
    )
)

stats = (
    windowed
    .groupBy(
        "source",
        F.when(F.col("rows_in_track_id") >= 13, "HIGH").when(F.col("rows_in_track_id") >= 7, "MEDIUM").otherwise("LOW").alias("rows_in_track_id_tier"),
    )
    .agg(
        F.countDistinct("id").alias("distinct_ids"),
        F.min("played_at").alias("first_played_at"),
        F.round(100 * F.count(F.when(F.col("device").isin("car", "desktop", "phone"), 1)) / F.count("*"), 1).alias("pct_device_in_car_desktop_phone"),
        F.min("ms_played").alias("min_ms_played"),
        F.count("*").alias("row_count"),
    )
)

qualified = (
    stats
    .crossJoin(stats.agg(F.max("pct_device_in_car_desktop_phone").alias("overall_max_pct_device_in_car_desktop_phone")))
    .filter(F.col("pct_device_in_car_desktop_phone") < F.col("overall_max_pct_device_in_car_desktop_phone"))
    .drop("overall_max_pct_device_in_car_desktop_phone")
)

result = (
    qualified
    .select(
        "source",
        "rows_in_track_id_tier",
        "distinct_ids",
        "first_played_at",
        "pct_device_in_car_desktop_phone",
        "min_ms_played",
        "row_count",
        F.abs(F.col("min_ms_played") - 182750).alias("min_ms_played_distance"),
        F.date_format(F.col("first_played_at"), "MMM yyyy").alias("first_played_at_label"),
    )
    .orderBy(
        "min_ms_played",
        F.col("distinct_ids").asc(),
        "source",
        "rows_in_track_id_tier",
    )
    .limit(10)
)
