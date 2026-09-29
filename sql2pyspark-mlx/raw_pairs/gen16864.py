from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("browser", "customer_id").orderBy(F.col("started_at").desc(), "id")
w_recent = Window.partitionBy(F.date_trunc("YEAR", "started_at")).orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_hist = Window.partitionBy("browser").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_grp = Window.partitionBy("customer_id").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.event_time").desc(), F.col("c.id").desc())

base = (
    df_sessions
    .filter((F.col("started_at") >= F.lit("2023-08-25 03:00:00").cast("timestamp")) & F.col("customer_id").isNotNull())
    .select("*", F.lower("device").alias("device_lower"))
)

scored = (
    base
    .select(
        "*",
        F.row_number().over(w).alias("seq_no"),
        F.last("country", ignorenulls=True).over(w_recent).alias("last_known_country"),
        F.first("browser", ignorenulls=True).over(w_hist).alias("first_known_browser"),
        F.first("browser", ignorenulls=True).over(w_hist).alias("first_known_browser_2"),
        F.last("country", ignorenulls=True).over(w_grp).alias("last_known_country_2"),
    )
)

with_last = (
    scored
    .alias("s")
    .join(df_web_events.alias("c"), (F.col("c.session_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w5))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.device",
        "s.browser",
        "s.started_at",
        "s.country",
        "s.first_known_browser",
        "s.first_known_browser_2",
        "s.last_known_country_2",
        F.col("c.page").alias("web_event_page"),
    )
)

matched_keys = (
    with_last
    .select("browser", "device")
    .subtract(
        df_sessions
        .filter(F.col("customer_id").isin([3, 4, 8, 26]))
        .select("browser", "device")
    )
)

kept_rows = (
    with_last
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.browser") == F.col("k.browser")) & (F.col("s.device") == F.col("k.device")), "inner")
    .select("s.*")
)

valid = (
    kept_rows
    .filter(F.col("web_event_page").isNotNull())
    .select(
        "device",
        "browser",
        "country",
        "first_known_browser",
        "first_known_browser_2",
        "last_known_country_2",
        "web_event_page",
        F.coalesce("country", F.lit("Unknown")).alias("country_filled"),
        F.trim("device").alias("clean_device"),
        F.coalesce("browser", F.lit("N/A")).alias("resolved_browser"),
    )
)

result = (
    valid
    .groupBy("browser")
    .agg(
        F.count(F.when(F.col("browser") == "Edge", 1)).alias("edge_rows"),
        F.count("*").alias("event_count"),
        F.sort_array(F.collect_set("web_event_page")).alias("web_event_page_list"),
        F.count("*").alias("row_count"),
        F.count_if(F.col("browser") == "Edge").alias("edge_events"),
        F.count(F.when(F.col("device") == "tablet", 1)).alias("tablet_count"),
    )
)
