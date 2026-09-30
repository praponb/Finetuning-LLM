from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("duration_sec", "id").rowsBetween(-6, Window.currentRow)
w2 = Window.partitionBy("session_id", "page").orderBy("duration_sec", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_grp = Window.partitionBy("event_type", "session_id").orderBy(F.col("event_time").cast("bigint")).rangeBetween(-604800, Window.currentRow)
w4 = Window.partitionBy("page").orderBy("event_time", "id")
w5 = Window.partitionBy("session_id").orderBy("duration_sec_max_7d", "id")
w6 = Window.partitionBy("event_type", "session_id").orderBy("event_time", "id")
w_ordered = Window.orderBy(F.col("duration_sec_max_7d").desc(), "id")

filtered = (
    df_web_events
    .filter((F.col("event_time") > F.lit("2023-08-04 11:00:00").cast("timestamp")) & F.col("page").isNotNull())
    .withColumn("event_type_title", F.initcap(F.trim("event_type")))
    .withColumn("clean_event_type", F.trim("event_type"))
)

enriched = (
    filtered
    .select(
        "*",
        F.avg("duration_sec").over(w).alias("duration_sec_avg_6"),
        F.first("duration_sec", ignorenulls=True).over(w2).alias("first_known_duration_sec"),
        F.max("duration_sec").over(w_grp).alias("duration_sec_max_7d"),
    )
)

ranked = (
    enriched
    .withColumn("latest_rank", F.row_number().over(w4))
    .filter(F.col("latest_rank") == 1)
)

with_dims = (
    ranked
    .alias("e")
    .join(df_sessions.alias("b"), F.col("e.session_id") == F.col("b.id"), "left")
    .select("e.*", F.col("b.device"))
)

enriched_web_events = (
    with_dims
    .select(
        "*",
        F.row_number().over(w5).alias("event_seq"),
        F.row_number().over(w6).alias("seq_no"),
        F.ntile(5).over(w_ordered).alias("duration_sec_max_7d_bucket"),
    )
)

with_dims_web_events = (
    enriched_web_events
    .alias("src")
    .join(df_sessions.alias("s"), F.col("src.session_id") == F.col("s.id"), "left")
    .select(
        "src.*",
        F.col("s.browser"),
        F.col("s.country"),
        F.col("s.device").alias("session_device"),
    )
)

result = (
    with_dims_web_events
    .select(
        "id",
        "session_id",
        "event_time",
        "event_type_title",
        "duration_sec_avg_6",
        "device",
        "event_seq",
        "seq_no",
        "duration_sec_max_7d_bucket",
    )
    .orderBy(F.col("duration_sec_max_7d_bucket").desc(), "event_seq")
)
