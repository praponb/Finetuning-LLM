from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.duration_sec").desc(), F.col("c.id").desc())
w2 = Window.partitionBy("web_event_event_type").orderBy(F.col("started_at").asc_nulls_last(), "id")

filtered = (
    df_sessions
    .filter((F.col("started_at") >= F.to_timestamp(F.lit("2023-09-12 16:00:00"))) & F.col("country").isNotNull())
    .select(
        "id",
        "device",
        "browser",
        "started_at",
        "country",
        F.concat("country", F.lit("/"), "device").alias("country_device"),
    )
)

latest_match = (
    filtered
    .alias("s")
    .join(df_web_events.alias("c"), (F.col("c.session_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.started_at",
        F.col("c.event_time").alias("web_event_event_time"),
        F.col("c.event_type").alias("web_event_event_type"),
    )
)

not_flagged = (
    latest_match
    .alias("s")
    .join(df_web_events.alias("x"), (F.col("x.session_id") == F.col("s.id")) & (F.col("x.event_time") >= F.col("s.started_at")), "left_semi")
)

latest = (
    not_flagged
    .withColumn("row_num", F.row_number().over(w2))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

result = (
    latest
    .groupBy(F.date_trunc("YEAR", "started_at").alias("year_start"))
    .agg(
        F.count(F.when(F.col("web_event_event_type") == "page_view", 1)).alias("page_view_rows"),
        F.sort_array(F.collect_set("web_event_event_type")).alias("web_event_event_type_list"),
        F.size(F.collect_set("web_event_event_type")).alias("num_web_event_event_types"),
    )
    .orderBy(F.col("page_view_rows").desc())
)
