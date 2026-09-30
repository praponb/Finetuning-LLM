from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("event_type").orderBy("duration_sec")
w2 = Window.partitionBy("session_id", "country").orderBy("duration_sec_rounded", "id")

cleaned = (
    df_web_events
    .filter(F.col("page").isNotNull())
    .select(
        "id",
        "session_id",
        "event_type",
        "event_time",
        "duration_sec",
        F.round("duration_sec", 1).alias("duration_sec_rounded"),
        F.trim("page").alias("clean_page"),
        F.coalesce("page", F.lit("N/A")).alias("page_clean"),
    )
)

deduped = (
    cleaned
    .withColumn("dedup_rank", F.rank().over(w))
    .filter(F.col("dedup_rank") == 1)
    .select("id", "session_id", "duration_sec_rounded")
)

enriched = (
    deduped
    .alias("src")
    .join(df_sessions.alias("d"), F.col("src.session_id") == F.col("d.id"), "left")
    .filter(F.col("d.country").isin(["UK", "JP", "SG"]))
    .select(
        "src.id",
        "src.session_id",
        "src.duration_sec_rounded",
        F.col("d.country"),
        F.col("d.device"),
    )
)

scored = (
    enriched
    .select(
        "id",
        "duration_sec_rounded",
        F.lead("duration_sec_rounded").over(w2).alias("next_duration_sec_rounded"),
        F.row_number().over(w2).alias("seq_no"),
    )
)

result = (
    scored
    .groupBy(
        F.when(F.col("duration_sec_rounded") >= 72, "PREMIUM").otherwise("STANDARD").alias("duration_sec_rounded_tier"),
    )
    .agg(
        F.percentile_approx("next_duration_sec_rounded", 0.95).alias("next_duration_sec_rounded_p95"),
        F.percentile_approx("seq_no", [0.25, 0.5, 0.75]).alias("seq_no_quartiles"),
        F.percentile_approx("seq_no", 0.5).alias("seq_no_p50"),
        F.percentile_approx("next_duration_sec_rounded", 0.5).alias("next_duration_sec_rounded_p50"),
        F.round(F.avg("next_duration_sec_rounded"), 2).alias("avg_next_duration_sec_rounded"),
    )
)
