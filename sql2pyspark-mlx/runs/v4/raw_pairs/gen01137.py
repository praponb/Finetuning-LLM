from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("event_type").orderBy(F.col("event_time").cast("bigint")).rangeBetween(-604800, Window.currentRow)
w_running = Window.partitionBy("page").orderBy(F.col("duration_sec").desc_nulls_last())
w3 = Window.partitionBy("page").orderBy("event_time", "id")
w_recent = Window.orderBy("duration_sec", "id")
w5 = Window.orderBy("event_time", "id")
w6 = Window.partitionBy("browser").orderBy("row_seq", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

enriched = (
    df_web_events
    .alias("a")
    .join(df_sessions.alias("d"), F.col("a.session_id") == F.col("d.id"), "left")
    .select("a.*", F.col("d.device"), F.col("d.browser"))
)

enriched_web_events = (
    enriched
    .withColumn("duration_sec_avg_7d", F.avg("duration_sec").over(w))
    .withColumn("duration_sec_cume_dist", F.cume_dist().over(w_running))
    .withColumn("row_seq", F.row_number().over(w3))
    .withColumn("prev_duration_sec", F.lag("duration_sec", 1, 0).over(w_recent))
)

metrics = (
    enriched_web_events
    .select(
        "*",
        F.lag("duration_sec", 2).over(w5).alias("prev_duration_sec_2"),
        F.sum("row_seq").over(w6).alias("cumulative_row_seq"),
    )
)

result = (
    metrics
    .groupBy(
        F.when(F.col("duration_sec_avg_7d") >= 466, "LARGE").otherwise("SMALL").alias("duration_sec_avg_7d_tier"),
        F.when(F.col("prev_duration_sec_2") >= 536, "LARGE").otherwise("SMALL").alias("prev_duration_sec_2_tier"),
        "session_id",
    )
    .agg(
        ((F.unix_timestamp(F.max("event_time")) - F.unix_timestamp(F.min("event_time"))) / 60).alias("duration_min"),
        F.avg(F.when(F.col("event_type") == "page_view", F.col("duration_sec_avg_7d"))).alias("page_view_avg_duration_sec_avg_7d"),
        F.sort_array(F.collect_set("page")).alias("page_list"),
        F.round(F.avg("row_seq"), 2).alias("avg_row_seq"),
        F.bit_and("duration_sec").alias("duration_sec_bit_and"),
        F.max("event_time").alias("last_event_time"),
    )
    .orderBy(F.col("prev_duration_sec_2_tier").desc_nulls_last())
)
