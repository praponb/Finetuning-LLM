from pyspark.sql import functions as F

filtered = (
    df_plays
    .filter((F.col("is_skipped")) | (F.to_date(F.col("played_at")) < F.lit("2025-02-18").cast("date")))
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

joined_listener = (
    filtered.alias("s")
    .join(df_listeners.alias("l"), F.col("l.id") == F.col("s.listener_id"), "inner")
    .filter(F.year(F.col("l.signup_date")) == 2025)
    .select(
        "s.id",
        "s.listener_id",
        "s.track_id",
        "s.played_at",
        "s.ms_played",
        "s.source",
        F.col("l.signup_date").alias("listener_signup_date"),
        F.col("l.interests").alias("listener_interests"),
        F.col("l.country").alias("listener_country"),
    )
)

grouped = (
    joined_listener
    .filter(F.year(F.col("listener_signup_date")) == 2025)
    .groupBy(
        "listener_country",
        F.date_format("listener_signup_date", "yyyy-MM").alias("listener_signup_date_ym"),
        F.to_date("played_at").alias("played_at_day"),
    )
    .agg(
        F.count(F.when(~F.col("source").isin("radio", "playlist"), 1)).alias("n_source_not_in_radio_playlist"),
        F.count(F.when(F.col("listener_country") != "JP", 1)).alias("n_listener_country_jp"),
        F.min("played_at").alias("first_played_at"),
        F.countDistinct("listener_country").alias("distinct_listener_countrys"),
        F.sum(F.when(~F.col("listener_country").isin("TH", "UK"), F.col("ms_played"))).alias("ms_played_listener_country_not_in_th_u"),
    )
)

result = (
    grouped
    .select(
        "listener_country",
        "listener_signup_date_ym",
        F.col("n_listener_country_jp").alias("n_listener_country_jp_v"),
        F.col("distinct_listener_countrys").alias("distinct_listener_countrys_v"),
        F.col("ms_played_listener_country_not_in_th_u").alias("m_ms_played_listener_country_not_in_th_u"),
    )
    .unpivot(
        ids=["listener_country", "listener_signup_date_ym"],
        values=["n_listener_country_jp_v", "distinct_listener_countrys_v", "m_ms_played_listener_country_not_in_th_u"],
        variableColumnName="field",
        valueColumnName="reading",
    )
    .filter(F.col("reading").isNotNull())
)
