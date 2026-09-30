from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("event_type").orderBy(F.col("duration_sec").desc_nulls_last())
w2 = Window.orderBy("duration_sec")
duration_sec_by_session_id = df_web_events.groupBy("session_id").agg(F.min("duration_sec").alias("min_web_event_duration_sec"))

enriched = (
    df_web_events
    .withColumn("duration_sec_cume_dist", F.cume_dist().over(w))
    .withColumn("duration_sec_percent_rank", F.percent_rank().over(w2))
)

result = (
    enriched
    .join(duration_sec_by_session_id, on="session_id", how="left")
    .select(
        "id",
        "session_id",
        "event_type",
        "event_time",
        "duration_sec",
        "duration_sec_cume_dist",
        F.when(F.col("event_type").rlike("^(purchase|page_view)$"), "CORE").otherwise("GROUP_A").alias("event_type_zone"),
        F.col("min_web_event_duration_sec"),
    )
    .orderBy("duration_sec")
)
