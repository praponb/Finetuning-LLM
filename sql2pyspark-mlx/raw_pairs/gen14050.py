from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("session_id").orderBy("event_time", "id")
w2 = Window.partitionBy("session_id").orderBy("event_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

filtered = (
    df_web_events
    .filter(F.col("event_time") > F.lit("2023-03-30").cast("date"))
    .select(
        "*",
        F.floor("duration_sec").alias("duration_sec_whole"),
        F.coalesce("page", F.lit("OTHER")).alias("resolved_page"),
    )
)

gaps = (
    filtered
    .withColumn("session_start_flag", F.when(F.lag("event_time").over(w).isNull() | (F.col("event_time") > F.lag("event_time").over(w) + F.expr("INTERVAL 7 DAYS")), 1).otherwise(0))
)

sessionized = gaps.withColumn("burst_id", F.sum("session_start_flag").over(w2))

result = (
    sessionized
    .groupBy("burst_id")
    .agg(
        F.count(F.when(F.col("event_type") == "click", 1)).alias("click_count"),
        F.count("*").alias("row_count"),
    )
    .filter(F.col("click_count") > 1)
)
