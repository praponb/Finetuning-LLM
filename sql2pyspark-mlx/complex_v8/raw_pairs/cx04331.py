from pyspark.sql import functions as F

filtered = df_listeners.filter((~F.col("country").isin("BR", "TH")) & (F.col("plan").isNotNull()))

play_stats = (
    df_plays
    .groupBy("listener_id")
    .agg(F.min("ms_played").alias("min_play_ms_played"))
)

summarized_rows = (
    filtered.alias("s")
    .join(play_stats.alias("p"), F.col("p.listener_id") == F.col("s.id"), "left")
    .select("s.*", "p.min_play_ms_played")
)

play_stats_2 = (
    df_plays
    .groupBy("listener_id")
    .agg(
        F.count(F.when(F.col("device").isNotNull(), 1)).alias("play_device_count"),
        F.count("*").alias("play_count"),
        F.min("played_at").alias("first_played_at"),
    )
)

summarized_rows_2 = (
    summarized_rows.alias("s")
    .join(play_stats_2.alias("p"), F.col("p.listener_id") == F.col("s.id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("p.play_device_count"), F.lit(0)).alias("play_device_count"),
        F.coalesce(F.col("p.play_count"), F.lit(0)).alias("play_count"),
        "p.first_played_at",
    )
)

annotated = (
    summarized_rows_2
    .filter(F.col("play_count") < 6)
    .select(
        "*",
        (F.col("min_play_ms_played") + F.col("birth_year")).alias("min_play_ms_played_plus_birth_year"),
        F.length(F.trim(F.col("plan"))).alias("plan_len"),
    )
)

result = (
    annotated
    .groupingSets(
        [["play_device_count", "plan"], ["play_device_count"], ["plan"], []],
        "play_device_count", "plan",
    )
    .agg(
        F.countDistinct("id").alias("distinct_ids"),
        F.array_sort(F.collect_set("country")).alias("country_list"),
        F.countDistinct("plan").alias("distinct_plans"),
    )
    .orderBy(
        F.col("play_device_count").asc_nulls_last(),
        F.col("plan").asc_nulls_last(),
    )
)
