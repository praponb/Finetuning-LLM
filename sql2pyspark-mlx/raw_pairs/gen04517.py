from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country").orderBy(F.col("started_at_quarter").desc())
w2 = Window.partitionBy("browser").orderBy("started_at_quarter", "id")
w3 = Window.orderBy("started_at_quarter", "id").rowsBetween(-2, Window.currentRow)
w4 = Window.partitionBy("device").orderBy(F.col("started_at_quarter").asc_nulls_first())
w5 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.duration_sec").desc(), F.col("c.id").desc())
w6 = Window.partitionBy("browser").orderBy("started_at", "id")
w7 = Window.partitionBy("browser").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w8 = Window.partitionBy("web_event_page").orderBy(F.col("web_event_duration_sec_per_home").desc())

filtered = (
    df_sessions
    .filter(F.col("started_at") >= F.lit("2023-04-16").cast("date"))
    .withColumn("device_group", F.when(F.col("device").isin(["desktop", "mobile"]), "Tier 1").otherwise("Other"))
    .withColumn("started_at_quarter", F.quarter("started_at"))
    .withColumn("country_clean", F.coalesce("country", F.lit("NONE")))
)

enriched = (
    filtered
    .withColumn("started_at_quarter_cume_dist", F.cume_dist().over(w))
    .withColumn("started_at_quarter_bucket", F.ntile(5).over(w2))
    .withColumn("started_at_quarter_avg_2", F.avg("started_at_quarter").over(w3))
    .withColumn("started_at_quarter_dense_rank", F.dense_rank().over(w4))
)

with_last = (
    enriched
    .alias("s")
    .join(df_web_events.alias("c"), (F.col("c.session_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w5))
    .filter(F.col("rn") == 1)
    .select(
        "s.*",
        F.col("c.page").alias("web_event_page"),
        F.col("c.duration_sec").alias("web_event_duration_sec"),
    )
)

gaps = (
    with_last
    .withColumn("is_new_session", F.when(F.lag("started_at").over(w6).isNull() | (F.col("started_at") > F.lag("started_at").over(w6) + F.expr("INTERVAL 14 DAYS")), 1).otherwise(0))
)

sessionized = (
    gaps
    .select(
        "id",
        "device",
        "browser",
        "started_at",
        "started_at_quarter",
        "country_clean",
        "started_at_quarter_cume_dist",
        "started_at_quarter_dense_rank",
        "web_event_page",
        F.sum("is_new_session").over(w7).alias("island_id"),
    )
)

with_last_sessions = (
    sessionized
    .alias("s")
    .join(df_web_events.alias("c"), (F.col("c.session_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w5))
    .filter(F.col("rn") == 1)
    .select(
        "s.*",
        F.col("c.duration_sec").alias("web_event_duration_sec"),
        F.col("c.event_time"),
    )
)

result = (
    with_last_sessions
    .groupBy(
        "web_event_page",
        F.date_trunc("quarter", "started_at").alias("quarter_start"),
    )
    .agg(
        F.round(F.avg("started_at_quarter_cume_dist"), 2).alias("avg_started_at_quarter_cume_dist"),
        F.round(F.sum("web_event_duration_sec") / F.nullif(F.count(F.when(F.col("web_event_page") == "/home", 1)), F.lit(0)), 2).alias("web_event_duration_sec_per_home"),
    )
    .withColumn("web_event_duration_sec_per_home_percent_rank", F.percent_rank().over(w8))
    .orderBy("quarter_start")
)
