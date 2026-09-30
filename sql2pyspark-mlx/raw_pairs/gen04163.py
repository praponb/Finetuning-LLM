from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("page", "session_id").orderBy(F.col("duration_sec").asc_nulls_first(), "id")
w2 = Window.partitionBy("page").orderBy("event_time", "id")
w3 = Window.partitionBy("page").orderBy("event_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("page").orderBy(F.col("duration_sec").desc())
w5 = Window.partitionBy("session_id", "event_type").orderBy(F.col("prev_event_time").cast("bigint")).rangeBetween(-7776000, Window.currentRow)
w6 = Window.partitionBy("page")
w7 = Window.partitionBy(F.year("event_time"), "event_type").orderBy("session_no", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

filtered = (
    df_web_events
    .filter((F.col("event_time") >= F.lit("2023-04-02 14:00:00").cast("timestamp")) & (F.col("page").eqNullSafe(F.lit("/home"))))
    .withColumn("clean_page", F.trim("page"))
    .withColumn("page_filled", F.coalesce("page", F.lit("DIRECT")))
)

ranked = (
    filtered
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

with_prev = ranked.withColumn("prev_event_time", F.lag("event_time").over(w2))

with_gaps = (
    with_prev
    .withColumn("is_new_burst", F.when(F.col("prev_event_time").isNull() | (F.col("event_time") > F.col("prev_event_time") + F.expr("INTERVAL 90 DAYS")), 1).otherwise(0))
)

sessionized = with_gaps.withColumn("session_no", F.sum("is_new_burst").over(w3))

deduped = (
    sessionized
    .withColumn("row_num", F.rank().over(w4))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

result = (
    deduped
    .select(
        "*",
        F.first("page", ignorenulls=True).over(w3).alias("first_known_page"),
        F.avg("is_new_burst").over(w5).alias("is_new_burst_avg_90d"),
        F.stddev_samp("is_new_burst").over(w6).alias("page_stddev_samp_is_new_burst"),
        F.sum("is_new_burst").over(w7).alias("running_is_new_burst"),
    )
)
