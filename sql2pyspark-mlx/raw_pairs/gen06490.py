from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("session_id").orderBy("event_time", "id")
w2 = Window.partitionBy("session_id").orderBy("event_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("duration_sec_num_tier").orderBy(F.col("purchase_avg_island_id").desc())

filtered = (
    df_web_events
    .filter(F.col("page").isNotNull())
    .select("*", F.col("duration_sec").cast("double").alias("duration_sec_num"))
)

with_gaps = (
    filtered
    .withColumn("new_group_flag", F.when(F.col("event_time") - F.lag("event_time").over(w) > F.expr("INTERVAL 60 DAYS"), 1).otherwise(0))
)

sessions_web_events = (
    with_gaps
    .select(
        "id",
        "session_id",
        "event_type",
        "event_time",
        "duration_sec_num",
        F.sum("new_group_flag").over(w2).alias("island_id"),
    )
)

result = (
    sessions_web_events
    .groupBy(
        F.when(F.col("duration_sec_num") >= 85, "LARGE").otherwise("SMALL").alias("duration_sec_num_tier"),
        F.when(F.col("island_id") >= 8, "LARGE").otherwise("SMALL").alias("island_id_tier"),
    )
    .agg(
        F.avg(F.when(F.col("event_type") == "purchase", F.col("island_id"))).alias("purchase_avg_island_id"),
        F.sum(F.when(F.col("event_type") == "search", F.col("duration_sec_num"))).alias("search_total_duration_sec_num"),
        F.count_if(F.col("event_type") == "search").alias("search_events"),
        F.size(F.collect_set("event_type")).alias("num_event_types"),
        F.percentile_approx("duration_sec_num", 0.9).alias("duration_sec_num_p90"),
    )
    .withColumn("purchase_avg_island_id_percent_rank", F.percent_rank().over(w3))
    .orderBy(F.col("duration_sec_num_p90").desc_nulls_first())
)
