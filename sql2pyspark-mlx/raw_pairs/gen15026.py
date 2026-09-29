from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.duration_sec").desc(), F.col("c.id").desc())

cleaned = (
    df_sessions
    .filter(F.col("started_at") >= "2024-03-01")
    .select(
        "id",
        "device",
        "browser",
        "country",
        F.lower("browser").alias("browser_lower"),
        F.substring("browser", 1, 3).alias("browser_prefix"),
        F.coalesce("country", F.lit("DIRECT")).alias("country_filled"),
    )
)

eligible = (
    cleaned
    .alias("s")
    .join(df_web_events.alias("ch"), (F.col("ch.session_id") == F.col("s.id")) & (F.col("ch.page").isin(["/search", "/product"])), "left_anti")
)

with_latest = (
    eligible
    .alias("s")
    .join(df_web_events.alias("c"), (F.col("c.session_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.page").alias("web_event_page"), F.col("c.duration_sec"))
)

kept = (
    with_latest
    .alias("s")
    .join(df_web_events.alias("x"), (F.col("x.session_id") == F.col("s.id")) & (F.col("x.page") != "/search"), "left_anti")
)

summary = (
    kept
    .rollup(
        F.when(F.col("duration_sec") >= 375, "HIGH").otherwise("LOW").alias("duration_sec_tier"),
        "country",
    )
    .agg(
        F.max(F.when(F.col("device") == "mobile", F.col("duration_sec"))).alias("mobile_max_duration_sec"),
        F.size(F.collect_set("web_event_page")).alias("num_web_event_pages"),
        F.sum(F.when(F.col("web_event_page") == "/product", F.col("duration_sec")).otherwise(0)).alias("product_duration_sec"),
        F.countDistinct("web_event_page").alias("distinct_web_event_pages"),
        F.min("duration_sec").alias("min_duration_sec"),
        F.count(F.when(F.col("web_event_page") == "/checkout", 1)).alias("checkout_rows"),
    )
    .filter(F.col("mobile_max_duration_sec") > 0)
)

result = (
    summary
    .select(
        "country",
        "mobile_max_duration_sec",
        "num_web_event_pages",
        "product_duration_sec",
        "min_duration_sec",
    )
    .orderBy(F.col("country").desc_nulls_last())
)
