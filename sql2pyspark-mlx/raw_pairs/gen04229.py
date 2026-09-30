from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("page").orderBy("event_time", "id")
w2 = Window.partitionBy("page").orderBy("event_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_grp = Window.partitionBy("browser").orderBy("session_start_flag", "id")
w4 = Window.orderBy("event_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

filtered = (
    df_web_events
    .filter((F.col("duration_sec") == 165) & (~F.col("page").eqNullSafe(F.lit("/checkout"))))
    .select(
        "*",
        F.coalesce("duration_sec", F.lit(0)).alias("duration_sec_filled"),
        F.coalesce("page", F.lit("DIRECT")).alias("page_clean"),
    )
)

with_gaps = (
    filtered
    .withColumn("session_start_flag", F.when(F.lag("event_time").over(w).isNull() | (F.col("event_time") > F.lag("event_time").over(w) + F.expr("INTERVAL 30 DAYS")), 1).otherwise(0))
)

sessions_web_events = with_gaps.withColumn("burst_id", F.sum("session_start_flag").over(w2))

joined = (
    sessions_web_events
    .alias("src")
    .join(df_sessions.alias("p"), F.col("src.session_id") == F.col("p.id"), "left")
    .select(
        "src.id",
        "src.session_id",
        "src.page",
        "src.event_time",
        "src.duration_sec_filled",
        "src.session_start_flag",
        F.col("p.browser"),
        F.col("p.device"),
    )
)

joined_web_events = (
    joined
    .alias("s")
    .join(df_sessions.alias("p"), F.col("s.session_id") == F.col("p.id"), "inner")
    .select(
        "s.*",
        F.col("p.device").alias("session_device"),
        F.col("p.country"),
        F.col("p.browser").alias("session_browser"),
    )
)

metrics = (
    joined_web_events
    .select(
        "id",
        "session_id",
        "page",
        "session_start_flag",
        "browser",
        "device",
        "session_device",
        F.lead("duration_sec_filled", 1).over(w_grp).alias("next_duration_sec_filled"),
        F.last("session_device", ignorenulls=True).over(w4).alias("last_known_session_device"),
    )
)

result = (
    metrics
    .select(
        "id",
        "page",
        "session_start_flag",
        "browser",
        "device",
        "session_device",
        "last_known_session_device",
        F.when(F.col("device").rlike("^(desktop|tablet)$"), "EXTENDED").otherwise("GROUP_B").alias("device_zone"),
    )
    .orderBy(F.col("session_device").desc_nulls_last(), F.col("device").desc())
)
