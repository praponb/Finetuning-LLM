from pyspark.sql import functions as F

filtered_plays = (
    df_plays
    .filter((F.to_date(F.col("played_at")) < F.lit("2025-02-06").cast("date")) & (F.col("listener_id") <= 12))
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "ms_played",
        "device",
        "is_skipped",
        "source",
    )
)

joined = (
    filtered_plays.alias("s")
    .join(df_listeners.alias("l"), F.col("l.id") == F.col("s.listener_id"), "inner")
    .select("s.*", F.col("l.plan").alias("listener_plan"))
)

with_details = (
    joined.alias("s")
    .join(df_listeners.alias("l"), (F.col("l.id") == F.col("s.listener_id")) & (~F.col("l.plan").isin("student", "premium")), "inner")
    .filter(F.col("l.birth_year") < 1990)
    .select(
        "s.*",
        F.col("l.plan").alias("listener_plan_2"),
        F.col("l.username").alias("listener_username"),
        F.col("l.birth_year").alias("listener_birth_year"),
    )
)

result = (
    with_details
    .select(
        "id",
        F.col("listener_birth_year").alias("m_listener_birth_year"),
        F.col("ms_played").alias("ms_played_v"),
    )
    .unpivot(
        ids=["id"],
        values=["m_listener_birth_year", "ms_played_v"],
        variableColumnName="metric",
        valueColumnName="value",
    )
    .filter(F.col("value").isNotNull())
)
