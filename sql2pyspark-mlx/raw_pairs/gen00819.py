from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.duration_sec").desc(), F.col("c.id").desc())

with_latest = (
    df_sessions
    .alias("s")
    .join(df_web_events.alias("c"), (F.col("c.session_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.duration_sec").alias("web_event_duration_sec"))
)

result = (
    with_latest
    .withColumn("year_start", F.date_trunc("YEAR", "started_at"))
    .withColumn("web_event_duration_sec_tier", F.when(F.col("web_event_duration_sec") >= 491, "PREMIUM").otherwise("STANDARD"))
    .groupingSets(
        [["browser", "year_start", "web_event_duration_sec_tier"], ["browser", "year_start"]],
        "browser",
        "year_start",
        "web_event_duration_sec_tier",
    )
    .agg(
        F.max("started_at").alias("last_started_at"),
        F.max("started_at").alias("last_seen"),
        F.percentile_approx("web_event_duration_sec", 0.95).alias("web_event_duration_sec_p95"),
        F.sort_array(F.collect_set("browser")).alias("browser_list"),
        F.max(F.when(F.col("browser") == "Edge", F.col("web_event_duration_sec"))).alias("edge_max_web_event_duration_sec"),
        F.grouping("browser").alias("grouping_id"),
    )
    .orderBy(F.col("year_start").asc_nulls_first())
)
