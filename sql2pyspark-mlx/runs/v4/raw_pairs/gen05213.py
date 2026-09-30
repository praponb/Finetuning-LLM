from pyspark.sql import functions as F

cleaned = (
    df_web_events
    .filter(F.col("event_time") >= F.lit("2023-07-12").cast("date"))
    .select(
        "id",
        "session_id",
        "page",
        "duration_sec",
        F.coalesce("duration_sec", F.lit(0)).alias("duration_sec_filled"),
        F.coalesce("duration_sec", F.lit(0)).alias("duration_sec_filled_2"),
        F.coalesce("page", F.lit("DIRECT")).alias("resolved_page"),
    )
)

joined = (
    cleaned
    .alias("a")
    .join(df_sessions.alias("p"), F.col("a.session_id") == F.col("p.id"), "inner")
    .select("a.*", F.col("p.country"), F.col("p.browser"))
)

cleaned_web_events = (
    joined
    .filter(F.col("page").isin(["/product", "/checkout"]))
    .withColumn("country_filled", F.coalesce("country", F.lit("unassigned")))
    .withColumn("browser_country", F.concat("browser", F.lit(" "), "country"))
    .withColumn("resolved_browser", F.coalesce("browser", F.lit("N/A")))
)

result = (
    cleaned_web_events
    .groupBy("page", "browser")
    .pivot("country", ["DE", "UK", "JP"])
    .agg(
        F.sum("duration_sec_filled_2").alias("value_sum"),
        F.count(F.lit(1)).alias("n"),
    )
)
