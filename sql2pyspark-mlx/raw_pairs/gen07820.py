from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("page").orderBy(F.col("event_time").cast("bigint")).rangeBetween(-2592000, Window.currentRow)
w2 = Window.partitionBy("session_id").orderBy(F.col("duration_sec").desc())
w3 = Window.orderBy(F.col("event_time").cast("bigint")).rangeBetween(-604800, Window.currentRow)
w4 = Window.partitionBy("session_id").orderBy(F.col("event_time").cast("bigint")).rangeBetween(-604800, Window.currentRow)
w5 = Window.orderBy("duration_sec_count_7d")
w6 = Window.partitionBy("event_type")
w7 = Window.partitionBy("session_country").orderBy(F.col("event_time").cast("bigint")).rangeBetween(-604800, Window.currentRow)
w8 = Window.partitionBy("session_country")

matched_keys = (
    df_web_events
    .select("id")
    .intersect(
        df_web_events
        .filter(F.col("page").isin(["/product", "/search"]))
        .select("id")
    )
)

kept_rows = (
    df_web_events
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

metrics = (
    kept_rows
    .select(
        "*",
        F.avg("duration_sec").over(w).alias("duration_sec_avg_30d"),
        F.rank().over(w2).alias("duration_sec_rank"),
        F.count("duration_sec").over(w3).alias("duration_sec_count_7d"),
    )
)

joined = (
    metrics
    .alias("a")
    .join(df_sessions.alias("p"), F.col("a.session_id") == F.col("p.id"), "left")
    .select("a.*", F.col("p.country"))
)

enriched = (
    joined
    .alias("src")
    .join(df_sessions.alias("p"), F.col("src.session_id") == F.col("p.id"), "left")
    .select(
        "src.session_id",
        "src.event_type",
        "src.event_time",
        "src.duration_sec",
        "src.duration_sec_avg_30d",
        "src.duration_sec_rank",
        "src.duration_sec_count_7d",
        F.col("p.browser"),
        F.col("p.country").alias("session_country"),
    )
)

base = (
    enriched
    .filter((F.col("event_time") >= F.lit("2023-08-07 21:00:00").cast("timestamp")) & F.col("session_country").isNotNull())
    .select(
        "session_id",
        "event_type",
        "event_time",
        "duration_sec_avg_30d",
        "duration_sec_rank",
        "duration_sec_count_7d",
        "browser",
        "session_country",
        F.lower("event_type").alias("event_type_lower"),
    )
)

enriched_web_events = (
    base
    .select(
        "session_id",
        "duration_sec_count_7d",
        "browser",
        "event_type_lower",
        F.count("duration_sec_avg_30d").over(w4).alias("duration_sec_avg_30d_count_7d"),
        F.percent_rank().over(w5).alias("duration_sec_count_7d_percent_rank"),
        F.min("duration_sec_count_7d").over(w6).alias("event_type_min_duration_sec_count_7d"),
        F.count("duration_sec_rank").over(w7).alias("duration_sec_rank_count_7d"),
        F.round((F.col("duration_sec_rank") / F.sum("duration_sec_rank").over(w8)), 4).alias("duration_sec_rank_share"),
    )
)

result = (
    enriched_web_events
    .select(
        "session_id",
        "duration_sec_rank_share",
        "duration_sec_count_7d_percent_rank",
    )
    .unpivot(
        ids=["session_id"],
        values=["duration_sec_rank_share", "duration_sec_count_7d_percent_rank"],
        variableColumnName="kpi",
        valueColumnName="reading",
    )
    .filter(F.col("reading").isNotNull())
)
