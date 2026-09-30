from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.duration_sec").desc(), F.col("c.id").desc())
w2 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.event_time").desc(), F.col("c.id").desc())
w3 = Window.partitionBy("page").orderBy(F.col("web_event_duration_sec").desc_nulls_last(), "id")

with_last = (
    df_sessions
    .alias("s")
    .join(df_web_events.alias("c"), (F.col("c.session_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.browser",
        "s.country",
        F.col("c.page").alias("web_event_page"),
        F.col("c.event_type").alias("web_event_event_type"),
    )
)

with_latest = (
    with_last
    .alias("s")
    .join(df_web_events.alias("c"), (F.col("c.session_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w2))
    .filter(F.col("rn") == 1)
    .select("s.id", "s.web_event_page", F.col("c.duration_sec"), F.col("c.page"))
)

with_last_sessions = (
    with_latest
    .alias("s")
    .join(df_web_events.alias("c"), (F.col("c.session_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w2))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.page",
        F.col("c.event_type"),
        F.col("c.duration_sec").alias("web_event_duration_sec"),
    )
)

kept = (
    with_last_sessions
    .alias("s")
    .join(df_web_events.alias("x"), (F.col("x.session_id") == F.col("s.id")) & (F.col("x.page").isNotNull()), "left_anti")
)

deduped = (
    kept
    .withColumn("rank_in_group", F.row_number().over(w3))
    .filter(F.col("rank_in_group") == 1)
    .select("id", "page", "event_type")
)

remaining_keys = (
    deduped
    .select("id")
    .exceptAll(
        df_sessions
        .filter(F.col("country") == "JP")
        .select("id")
    )
)

kept_rows = (
    deduped
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

eligible = (
    kept_rows
    .alias("s")
    .join(df_web_events.alias("x"), (F.col("x.session_id") == F.col("s.id")) & (F.col("x.session_id") == 22), "left_anti")
)

result = (
    eligible
    .groupBy("event_type")
    .agg(
        F.size(F.collect_set("event_type")).alias("num_event_types"),
        F.count(F.when(F.col("event_type") == "purchase", 1)).alias("purchase_rows"),
        F.count(F.when(F.col("event_type") == "search", 1)).alias("search_rows"),
    )
    .orderBy(F.col("search_rows").desc_nulls_last())
)
