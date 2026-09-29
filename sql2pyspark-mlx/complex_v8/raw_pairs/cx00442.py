from pyspark.sql import functions as F

cleaned = (
    df_listeners
    .select(
        "id",
        "username",
        "plan",
        "country",
        "signup_date",
        "birth_year",
        F.concat(F.col("plan"), F.lit("#"), F.col("id").cast("string")).alias("plan_ref"),
        F.coalesce(F.col("birth_year"), F.lit(0)).alias("birth_year_filled"),
    )
)

with_activity = (
    cleaned.alias("s")
    .join(df_plays.alias("p"), F.col("p.listener_id") == F.col("s.id"), "left_semi")
)

enriched = (
    with_activity
    .filter(F.col("country") == "BR")
    .select(
        "*",
        F.abs(F.col("birth_year_filled") - 1994).alias("birth_year_filled_distance"),
        F.coalesce(F.col("birth_year"), F.lit(0)).alias("birth_year_filled_2"),
        F.date_format(F.col("signup_date"), "MMM yyyy").alias("signup_date_label"),
    )
)

with_children = (
    enriched.alias("s")
    .join(df_plays.alias("p"), (F.col("p.listener_id") == F.col("s.id")) & (F.col("p.device") != "car"), "left")
    .select(
        "s.id",
        "s.username",
        "s.country",
        "s.birth_year",
        "s.birth_year_filled_distance",
        F.col("p.id").alias("play_id"),
        F.col("p.played_at").alias("play_played_at"),
        F.col("p.device").alias("play_device"),
        F.col("p.source").alias("play_source"),
        F.col("p.ms_played").alias("play_ms_played"),
    )
)

per_group = (
    with_children
    .groupBy(
        "play_device",
        "play_id",
        F.date_format("play_played_at", "yyyy-MM").alias("play_played_at_ym"),
    )
    .agg(
        F.count("*").alias("cnt"),
        F.round(F.sum("birth_year_filled_distance") / F.nullif(F.sum("play_ms_played"), F.lit(0)), 3).alias("birth_year_filled_distance_to_play_ms_played_rat"),
        F.max("play_ms_played").alias("max_play_ms_played"),
    )
)

enriched_2 = (
    per_group
    .filter(F.col("play_device").isNotNull())
    .withColumn("cnt_tier", F.when(F.col("cnt") >= 10, "A").when(F.col("cnt") >= 6, "B").otherwise("C"))
)

tagged = enriched_2.select("*", F.lower(F.trim(F.col("cnt_tier"))).alias("cnt_tier_clean"))

result = (
    tagged
    .groupBy("play_device", "play_id")
    .pivot("cnt_tier", ["C", "B", "A"])
    .agg(F.sum("max_play_ms_played"))
    .orderBy("play_device", "play_id")
)
