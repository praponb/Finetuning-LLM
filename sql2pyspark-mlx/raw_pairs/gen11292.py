from pyspark.sql import functions as F

kept_keys = (
    df_web_events
    .select("session_id")
    .subtract(
        df_web_events
        .filter(F.col("event_time") < "2023-11-01")
        .select("session_id")
    )
)

kept_rows = (
    df_web_events
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.session_id") == F.col("k.session_id")), "inner")
    .select("s.*")
)

result = (
    kept_rows
    .groupBy(
        F.date_trunc("YEAR", "event_time").alias("year_start"),
        F.date_trunc("QUARTER", "event_time").alias("quarter_start"),
    )
    .agg(
        F.count(F.when(F.col("page") == "/product", 1)).alias("product_count"),
        F.avg(F.when(F.col("event_type") == "click", F.col("duration_sec"))).alias("click_avg_duration_sec"),
        F.size(F.collect_set("page")).alias("num_pages"),
    )
    .orderBy(F.col("product_count").desc())
)
