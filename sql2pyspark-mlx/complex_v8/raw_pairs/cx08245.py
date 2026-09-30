from pyspark.sql import functions as F

desktop_by_listener = (
    df_plays
    .filter(F.col("device") == "desktop")
    .groupBy("listener_id")
    .agg(
        F.count("*").alias("desktop_count"),
        F.sum("ms_played").alias("desktop_total_ms_played"),
    )
)

phone_by_listener = (
    df_plays
    .filter(F.col("device") == "phone")
    .groupBy("listener_id")
    .agg(
        F.count("*").alias("phone_count"),
        F.sum("ms_played").alias("phone_total_ms_played"),
    )
)

result = (
    desktop_by_listener.alias("a")
    .join(phone_by_listener.alias("b"), F.col("a.listener_id") == F.col("b.listener_id"), "full")
    .select(
        F.coalesce(F.col("a.listener_id"), F.col("b.listener_id")).alias("listener_id"),
        F.coalesce(F.col("a.desktop_count"), F.lit(0)).alias("desktop_count"),
        F.col("a.desktop_total_ms_played"),
        F.coalesce(F.col("b.phone_count"), F.lit(0)).alias("phone_count"),
        F.col("b.phone_total_ms_played"),
        (F.coalesce(F.col("a.desktop_count"), F.lit(0)) - F.coalesce(F.col("b.phone_count"), F.lit(0))).alias("desktop_count_minus_phone_count"),
    )
    .orderBy("listener_id")
)
