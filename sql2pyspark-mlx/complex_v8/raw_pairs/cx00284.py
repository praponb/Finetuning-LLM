from pyspark.sql import functions as F

phone_by_listener = (
    df_plays
    .filter(F.col("device") == "phone")
    .groupBy("listener_id")
    .agg(
        F.count("*").alias("phone_count"),
        F.sum("ms_played").alias("phone_total_ms_played"),
    )
)

desktop_by_listener = (
    df_plays
    .filter(F.col("device") == "desktop")
    .groupBy("listener_id")
    .agg(
        F.count("*").alias("desktop_count"),
        F.sum("ms_played").alias("desktop_total_ms_played"),
    )
)

result = (
    phone_by_listener.alias("a")
    .join(desktop_by_listener.alias("b"), F.col("a.listener_id") == F.col("b.listener_id"), "full")
    .select(
        F.coalesce(F.col("a.listener_id"), F.col("b.listener_id")).alias("listener_id"),
        F.coalesce(F.col("a.phone_count"), F.lit(0)).alias("phone_count"),
        F.coalesce(F.col("a.phone_total_ms_played"), F.lit(0)).alias("phone_total_ms_played"),
        F.coalesce(F.col("b.desktop_count"), F.lit(0)).alias("desktop_count"),
        F.coalesce(F.col("b.desktop_total_ms_played"), F.lit(0)).alias("desktop_total_ms_played"),
        (F.coalesce(F.col("a.phone_count"), F.lit(0)) - F.coalesce(F.col("b.desktop_count"), F.lit(0))).alias("net_count"),
    )
    .orderBy("listener_id")
)
