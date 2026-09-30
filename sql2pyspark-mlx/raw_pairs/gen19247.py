from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("browser").orderBy("started_at", "id")
w2 = Window.partitionBy("browser", "device").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.duration_sec").desc(), F.col("c.id").desc())
w4 = Window.partitionBy("browser").orderBy("event_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("country", "page").orderBy("event_time", "id").rowsBetween(-5, Window.currentRow)
w6 = Window.orderBy("seq_no", "id")
w7 = Window.partitionBy("device").orderBy("seq_no_2")
w8 = Window.partitionBy("page")
w9 = Window.partitionBy("page", "browser")

enriched = (
    df_sessions
    .select(
        "id",
        "device",
        "browser",
        "started_at",
        "country",
        F.row_number().over(w).alias("seq_no"),
        F.first("browser", ignorenulls=True).over(w2).alias("first_known_browser"),
    )
)

with_latest = (
    enriched
    .alias("s")
    .join(df_web_events.alias("c"), (F.col("c.session_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w3))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.event_time"), F.col("c.page"))
)

with_windows = (
    with_latest
    .select(
        "id",
        "device",
        "browser",
        "first_known_browser",
        "page",
        F.last("browser", ignorenulls=True).over(w4).alias("last_known_browser"),
        F.avg("seq_no").over(w5).alias("seq_no_avg_5"),
        F.row_number().over(w6).alias("seq_no_2"),
    )
)

remaining_keys = (
    with_windows
    .select("device")
    .subtract(
        df_sessions
        .filter(F.col("started_at") >= "2023-12-13")
        .select("device")
    )
)

reconciled = (
    with_windows
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.device") == F.col("k.device")), "inner")
    .select("s.*")
)

enriched_sessions = (
    reconciled
    .filter(~F.col("browser").isin(["Firefox", "Chrome"]))
    .select(
        "device",
        "browser",
        "page",
        "seq_no_avg_5",
        F.cume_dist().over(w7).alias("seq_no_2_cume_dist"),
        F.round((F.col("seq_no_2") / F.sum("seq_no_2").over(w8)), 4).alias("seq_no_2_share"),
        F.round((F.col("seq_no_avg_5") / F.sum("seq_no_avg_5").over(w9)), 4).alias("seq_no_avg_5_share"),
    )
)

matched_keys = (
    enriched_sessions
    .select("device", "browser")
    .exceptAll(
        df_sessions
        .filter(F.col("browser") == "Chrome")
        .select("device", "browser")
    )
)

kept_rows = (
    enriched_sessions
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.device") == F.col("k.device")) & (F.col("s.browser") == F.col("k.browser")), "inner")
    .select("s.*")
)

cleaned = (
    kept_rows
    .filter(~F.col("device").isin(["tablet", "desktop"]))
    .withColumn("device_browser_key", F.concat_ws("-", "device", "browser"))
)

result = (
    cleaned
    .groupBy("browser", "page")
    .pivot("device", ["tablet", "desktop", "mobile"])
    .agg(F.sum("seq_no_2_cume_dist").alias("value_sum"))
)
