from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.year("event_time"), "event_type").orderBy(F.col("duration_sec").asc_nulls_last())
w2 = Window.partitionBy("session_id")
w_hist = Window.partitionBy("country").orderBy("session_id_max_duration_sec", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("country")

enriched = (
    df_web_events
    .alias("s")
    .join(df_sessions.alias("d"), F.col("s.session_id") == F.col("d.id"), "left")
    .select("s.*", F.col("d.browser"), F.col("d.country"))
)

metrics = (
    enriched
    .filter(F.col("event_type") == "purchase")
    .withColumn("duration_sec_rank", F.rank().over(w))
    .withColumn("session_id_max_duration_sec", F.max("duration_sec").over(w2))
)

enriched_web_events = (
    metrics
    .select(
        "id",
        "event_type",
        "page",
        "duration_sec",
        "country",
        "duration_sec_rank",
        F.count("duration_sec").over(w_hist).alias("running_count_duration_sec"),
        F.min("duration_sec").over(w_hist).alias("running_min_duration_sec"),
        F.max("duration_sec").over(w_hist).alias("running_max_duration_sec"),
    )
)

matched_keys = (
    enriched_web_events
    .select("event_type", "page")
    .subtract(
        df_web_events
        .filter(F.col("duration_sec") >= 275)
        .select("event_type", "page")
    )
)

kept_rows = (
    enriched_web_events
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.event_type") == F.col("k.event_type")) & (F.col("s.page") == F.col("k.page")), "inner")
    .select("s.*")
)

scored = (
    kept_rows
    .select(
        "id",
        "event_type",
        "page",
        "country",
        "duration_sec_rank",
        F.avg("duration_sec").over(w4).alias("country_avg_duration_sec"),
    )
)

result = (
    scored
    .groupBy(
        F.when(F.col("country_avg_duration_sec") >= 454, "HIGH").otherwise("LOW").alias("country_avg_duration_sec_tier"),
    )
    .agg(
        F.countDistinct("country").alias("distinct_countries"),
        F.percentile_approx("country_avg_duration_sec", 0.9).alias("country_avg_duration_sec_p90"),
        F.sum("country_avg_duration_sec").alias("total_country_avg_duration_sec"),
    )
    .orderBy(F.col("total_country_avg_duration_sec").asc_nulls_last())
)
