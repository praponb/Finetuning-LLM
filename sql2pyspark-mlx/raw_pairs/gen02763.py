from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.event_time").desc(), F.col("c.id").desc())
w2 = Window.orderBy("web_event_duration_sec", "id")
w3 = Window.partitionBy("device").orderBy("web_event_duration_sec", "id").rowsBetween(-6, -1)
w4 = Window.partitionBy("country").orderBy("web_event_duration_sec", "id")

cleaned = (
    df_sessions
    .filter(F.col("device").isin(["desktop", "tablet"]) & (F.col("started_at") > F.lit("2023-06-12").cast("date")))
    .select(
        "*",
        F.initcap(F.trim("country")).alias("country_title"),
        F.coalesce("country", F.lit("N/A")).alias("country_filled"),
    )
)

not_flagged = (
    cleaned
    .alias("s")
    .join(df_web_events.alias("x"), (F.col("x.session_id") == F.col("s.id")) & (F.col("x.event_time") >= F.col("s.started_at")), "left_anti")
)

joined = (
    not_flagged
    .alias("e")
    .join(df_customers.alias("c"), F.col("e.customer_id") == F.col("c.id"), "inner")
    .filter(F.col("c.country") != "UK")
    .select(
        "e.id",
        "e.customer_id",
        "e.device",
        "e.browser",
        "e.country",
        "e.country_title",
        "e.country_filled",
        F.col("c.name"),
    )
)

with_latest = (
    joined
    .alias("s")
    .join(df_web_events.alias("c"), (F.col("c.session_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select(
        "s.*",
        F.col("c.page").alias("web_event_page"),
        F.col("c.duration_sec").alias("web_event_duration_sec"),
    )
)

metrics = (
    with_latest
    .select(
        "id",
        "browser",
        "country_filled",
        "name",
        F.lag("web_event_duration_sec", 2).over(w2).alias("prev_web_event_duration_sec"),
        F.avg("web_event_duration_sec").over(w3).alias("web_event_duration_sec_avg_6_prior"),
        F.lag("web_event_duration_sec", 2).over(w4).alias("prev_web_event_duration_sec_2"),
    )
)

base = (
    metrics
    .filter(F.col("browser").isNotNull() & (F.col("web_event_duration_sec_avg_6_prior") != 352))
    .select(
        "id",
        "browser",
        "country_filled",
        "name",
        "prev_web_event_duration_sec",
        "prev_web_event_duration_sec_2",
        F.substring("name", 1, 2).alias("name_prefix"),
        F.initcap(F.trim("name")).alias("name_title"),
        F.coalesce("browser", F.lit("N/A")).alias("browser_filled"),
    )
)

eligible = (
    base
    .alias("s")
    .join(df_web_events.alias("ch"), (F.col("ch.session_id") == F.col("s.id")) & (F.col("ch.session_id").isin([3, 10, 21, 23])), "left_semi")
)

result = (
    eligible
    .withColumn("prev_web_event_duration_sec_2_tier", F.when(F.col("prev_web_event_duration_sec_2") >= 375, "LARGE").otherwise("SMALL"))
    .groupingSets(
        [["browser", "prev_web_event_duration_sec_2_tier"], ["browser"], []],
        "browser",
        "prev_web_event_duration_sec_2_tier",
    )
    .agg(
        F.round(F.sum("prev_web_event_duration_sec") / F.nullif(F.count(F.when(F.col("browser") == "Safari", 1)), F.lit(0)), 2).alias("prev_web_event_duration_sec_per_safari"),
        F.sum("prev_web_event_duration_sec_2").alias("total_prev_web_event_duration_sec_2"),
        F.countDistinct("id").alias("distinct_ids"),
        F.grouping("browser").alias("gid"),
    )
    .orderBy(F.col("prev_web_event_duration_sec_per_safari").asc_nulls_last())
)
