from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("page").orderBy(F.col("duration_sec").desc_nulls_last(), "id")

with_dims = (
    df_web_events
    .alias("s")
    .join(df_sessions.alias("p2"), F.col("s.session_id") == F.col("p2.id"), "inner")
    .select("s.*", F.col("p2.browser"), F.col("p2.device"), F.col("p2.country"))
)

ranked = (
    with_dims
    .withColumn("rank_in_group", F.row_number().over(w))
    .filter(F.col("rank_in_group") == 1)
    .drop("rank_in_group")
)

with_dims_web_events = (
    ranked
    .alias("e")
    .join(df_sessions.alias("p"), F.col("e.session_id") == F.col("p.id"), "left")
    .select("e.*", F.col("p.browser").alias("session_browser"))
)

kept_keys = (
    with_dims_web_events
    .select("event_type")
    .subtract(
        df_web_events
        .filter(F.col("duration_sec").between(348, 366))
        .select("event_type")
    )
)

verified = (
    with_dims_web_events
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.event_type") == F.col("k.event_type")), "inner")
    .select("s.*")
)

base = (
    verified
    .filter((F.col("event_time") > F.to_timestamp(F.lit("2023-09-08 21:00:00"))) & F.col("session_browser").isNotNull())
    .select(
        "id",
        "session_id",
        "event_type",
        "page",
        "event_time",
        "duration_sec",
        "browser",
        "session_browser",
        F.to_date("event_time").alias("event_time_date"),
        F.coalesce("browser", F.lit("UNKNOWN")).alias("browser_clean"),
    )
)

joined = (
    base
    .alias("e")
    .join(df_sessions.alias("s"), F.col("e.session_id") == F.col("s.id"), "inner")
    .filter(F.col("s.device").isin(["tablet", "desktop"]))
    .select("e.*", F.col("s.browser").alias("session_browser_2"), F.col("s.country"))
)

enriched = (
    joined
    .alias("a")
    .join(df_sessions.alias("d"), F.col("a.session_id") == F.col("d.id"), "inner")
    .select(
        "a.session_id",
        "a.event_type",
        "a.browser",
        "a.event_time_date",
        "a.browser_clean",
        F.col("d.browser").alias("session_browser_3"),
        F.col("d.country").alias("session_country"),
        F.col("d.device"),
    )
)

result = (
    enriched
    .groupBy(
        "event_type",
        "session_id",
        F.date_trunc("MONTH", "event_time_date").alias("activity_month"),
    )
    .agg(
        F.count(F.when(F.col("session_browser_3") == "Chrome", 1)).alias("chrome_count"),
        F.count_if(F.col("event_type") == "click").alias("click_events"),
        F.count_if(F.col("session_country") == "UK").alias("uk_events"),
        F.count(F.when(F.col("event_type") == "add_to_cart", 1)).alias("add_to_cart_count"),
        F.count(F.when(F.col("device") == "desktop", 1)).alias("desktop_count"),
    )
)
