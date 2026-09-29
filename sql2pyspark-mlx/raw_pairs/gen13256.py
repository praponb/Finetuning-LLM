from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("page").orderBy("event_time", "id")
w2 = Window.partitionBy("page").orderBy("event_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

valid = (
    df_web_events
    .filter(F.col("page").isNotNull() & (~F.col("page").eqNullSafe(F.lit("/search"))))
    .select(
        "*",
        F.initcap(F.trim("page")).alias("page_title"),
        F.floor("duration_sec").alias("duration_sec_whole"),
    )
)

with_gaps = (
    valid
    .withColumn("new_group_flag", F.when(F.lag("event_time").over(w).isNull() | (F.col("event_time") > F.lag("event_time").over(w) + F.expr("INTERVAL 7 DAYS")), 1).otherwise(0))
)

sessionized = (
    with_gaps
    .select(
        "id",
        "session_id",
        "page",
        "event_time",
        "page_title",
        "duration_sec_whole",
        F.sum("new_group_flag").over(w2).alias("burst_id"),
    )
)

result = (
    sessionized
    .groupBy(
        F.when(F.col("duration_sec_whole") >= 90, "PREMIUM").otherwise("STANDARD").alias("duration_sec_whole_tier"),
    )
    .agg(
        F.min("event_time").alias("first_event_time"),
        F.avg(F.when(F.col("page") == "/home", F.col("burst_id"))).alias("home_avg_burst_id"),
    )
)
