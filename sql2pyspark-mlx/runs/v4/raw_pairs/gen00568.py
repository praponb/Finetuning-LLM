from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("page").orderBy("event_time", "id")
w2 = Window.partitionBy("page").orderBy("event_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("session_id").orderBy(F.col("event_time").cast("bigint")).rangeBetween(-604800, Window.currentRow)
w4 = Window.partitionBy("session_id").orderBy("prev_event_time", "id").rowsBetween(-6, Window.currentRow)
w5 = Window.partitionBy("event_type").orderBy("session_id_2", "id")
w6 = Window.partitionBy("event_type", "page").orderBy(F.col("duration_sec_sum_7d").asc_nulls_first(), "id")

filtered = (
    df_web_events
    .filter((F.col("event_time") > F.to_timestamp(F.lit("2023-08-18 00:00:00"))) & F.col("page").isNotNull())
    .withColumn("event_type_page", F.concat("event_type", F.lit(", "), "page"))
    .withColumn("resolved_page", F.coalesce("page", F.lit("NONE")))
)

prev_events = filtered.withColumn("prev_event_time", F.lag("event_time").over(w))

with_gaps = (
    prev_events
    .withColumn("new_group_flag", F.when(F.col("prev_event_time").isNull() | (F.col("event_time") > F.col("prev_event_time") + F.expr("INTERVAL 14 DAYS")), 1).otherwise(0))
)

sessionized = with_gaps.withColumn("session_id_2", F.sum("new_group_flag").over(w2))

scored = (
    sessionized
    .withColumn("duration_sec_sum_7d", F.sum("duration_sec").over(w3))
    .withColumn("duration_sec_avg_6", F.avg("duration_sec").over(w4))
    .withColumn("row_seq", F.row_number().over(w5))
)

ranked = (
    scored
    .withColumn("latest_rank", F.row_number().over(w6))
    .filter(F.col("latest_rank") == 1)
)

result = (
    ranked
    .groupBy("session_id")
    .pivot("page", ["/home", "/search"])
    .agg(F.avg("duration_sec_sum_7d").alias("total"), F.count(F.lit(1)).alias("cnt"))
    .select(
        "session_id",
        F.col("/home_total").alias("home_total"),
        F.col("/home_cnt").alias("home_cnt"),
        F.col("/search_total").alias("search_total"),
        F.col("/search_cnt").alias("search_cnt"),
    )
)
