from pyspark.sql import functions as F

filtered = (
    df_sessions
    .filter((F.col("started_at") >= F.to_date(F.lit("2023-09-29"))) & F.col("country").isNotNull())
    .select(
        "*",
        F.when(F.col("country").isin(["JP", "DE"]), "Tier 1").otherwise("Other").alias("country_group"),
    )
)

result = (
    filtered
    .alias("s")
    .join(df_web_events.alias("ch"), (F.col("ch.session_id") == F.col("s.id")) & (F.col("ch.duration_sec") <= 197), "left_anti")
)
