from pyspark.sql import functions as F

kept_keys = (
    df_web_events
    .select("id")
    .subtract(
        df_web_events
        .filter(F.col("event_type") != "page_view")
        .select("id")
    )
)

kept_rows = (
    df_web_events
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

result = kept_rows.select("id", "event_type", "page", "duration_sec")
