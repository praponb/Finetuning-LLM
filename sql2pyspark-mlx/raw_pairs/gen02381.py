from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_hist = Window.partitionBy("page")
w2 = Window.partitionBy(F.to_date("event_time"), "session_id").orderBy(F.col("duration_sec").desc(), "id")
w_hist_2 = Window.partitionBy("page").orderBy(F.col("event_time").cast("bigint")).rangeBetween(-7776000, Window.currentRow)
w_grp = Window.partitionBy(F.year("event_time"), "event_type")

enriched = (
    df_web_events
    .withColumn("page_avg_duration_sec", F.avg("duration_sec").over(w_hist))
    .withColumn("duration_sec_bucket", F.ntile(4).over(w2))
    .withColumn("duration_sec_count_90d", F.count("duration_sec").over(w_hist_2))
    .withColumn("page_min_duration_sec", F.min("duration_sec").over(w_hist))
    .withColumn("event_time_year_max_duration_sec", F.max("duration_sec").over(w_grp))
)

result = (
    enriched
    .groupBy("event_type")
    .agg(
        F.min("event_time").alias("first_seen"),
        F.min("event_time").alias("first_event_time"),
        F.round(F.avg("duration_sec_count_90d"), 2).alias("avg_duration_sec_count_90d"),
    )
    .filter(F.col("avg_duration_sec_count_90d") >= 0)
    .orderBy("avg_duration_sec_count_90d")
)
