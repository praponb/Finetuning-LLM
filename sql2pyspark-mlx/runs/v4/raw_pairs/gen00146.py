from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("page").orderBy("event_time", "id")
w2 = Window.partitionBy("page").orderBy("event_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("event_type")
w4 = Window.partitionBy("session_id").orderBy(F.col("event_time").cast("bigint")).rangeBetween(-2592000, Window.currentRow)
w5 = Window.orderBy("session_no_times_session_start_flag", "id")

with_gaps = (
    df_web_events
    .withColumn("session_start_flag", F.when(F.lag("event_time").over(w).isNull() | (F.col("event_time") > F.lag("event_time").over(w) + F.expr("INTERVAL 90 DAYS")), 1).otherwise(0))
)

islands = with_gaps.withColumn("session_no", F.sum("session_start_flag").over(w2))

cleaned = (
    islands
    .filter((F.col("event_time") > F.lit("2023-12-10").cast("date")) & F.col("page").isNotNull())
    .select(
        "id",
        "session_id",
        "event_type",
        "event_time",
        "duration_sec",
        "session_start_flag",
        "session_no",
        (F.col("session_no") * F.col("session_start_flag")).alias("session_no_times_session_start_flag"),
        F.coalesce("page", F.lit("OTHER")).alias("page_clean"),
    )
)

scored = (
    cleaned
    .select(
        "*",
        F.count("session_start_flag").over(w3).alias("event_type_count_session_start_flag"),
        F.avg("session_no").over(w4).alias("session_no_avg_30d"),
        F.ntile(10).over(w5).alias("session_no_times_session_start_flag_bucket"),
        F.count("session_no").over(w3).alias("event_type_count_session_no"),
    )
)

result = (
    scored
    .select(
        "id",
        "session_id",
        "event_type",
        "event_time",
        "duration_sec",
        "session_start_flag",
        "session_no_times_session_start_flag",
        "page_clean",
        "event_type_count_session_start_flag",
    )
    .orderBy(F.col("id").desc(), "duration_sec")
    .limit(10)
)
