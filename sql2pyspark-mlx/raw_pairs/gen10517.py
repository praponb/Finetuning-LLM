from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_hist = Window.partitionBy(F.date_trunc("month", "event_time"), "event_type").orderBy(F.col("duration_sec").desc(), "id")
w2 = Window.partitionBy("session_id", "event_type").orderBy(F.col("duration_sec").desc_nulls_last())
w3 = Window.partitionBy("page").orderBy("duration_sec", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
duration_sec_by_session_id = df_web_events.groupBy("session_id").agg(F.avg("duration_sec").alias("avg_web_event_duration_sec"))

with_windows = (
    df_web_events
    .select(
        "*",
        F.ntile(10).over(w_hist).alias("duration_sec_bucket"),
        F.percent_rank().over(w2).alias("duration_sec_percent_rank"),
        F.first("duration_sec", ignorenulls=True).over(w3).alias("first_known_duration_sec"),
    )
)

with_dims = (
    with_windows
    .alias("src")
    .join(df_sessions.alias("b"), F.col("src.session_id") == F.col("b.id"), "left")
    .select("src.*", F.col("b.country"), F.col("b.device"), F.col("b.browser"))
)

joined = (
    with_dims
    .alias("src")
    .join(df_sessions.alias("d"), F.col("src.session_id") == F.col("d.id"), "inner")
    .filter(F.col("d.country") == "UK")
    .select(
        "src.*",
        F.col("d.device").alias("session_device"),
        F.col("d.browser").alias("session_browser"),
    )
)

remaining_keys = (
    joined
    .select("event_type", "page")
    .intersect(
        df_web_events
        .filter(F.col("session_id") == 19)
        .select("event_type", "page")
    )
)

verified = (
    joined
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.event_type") == F.col("k.event_type")) & (F.col("s.page") == F.col("k.page")), "inner")
    .select("s.*")
)

result = (
    verified
    .join(duration_sec_by_session_id, on="session_id", how="left")
    .select(
        "id",
        "session_id",
        "event_type",
        "page",
        "event_time",
        "duration_sec",
        "duration_sec_bucket",
        "device",
        "browser",
        F.when(F.col("browser").rlike("^(Firefox|Chrome)$"), "CORE").otherwise("GROUP_A").alias("browser_zone"),
        F.col("avg_web_event_duration_sec"),
    )
)
