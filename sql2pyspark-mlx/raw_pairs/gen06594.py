from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("session_id").orderBy(F.col("cnt").desc())

matched_keys = (
    df_web_events
    .select("event_type", "session_id")
    .intersect(
        df_web_events
        .filter(F.hour("event_time").between(11, 15))
        .select("event_type", "session_id")
    )
)

reconciled = (
    df_web_events
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.event_type") == F.col("k.event_type")) & (F.col("s.session_id") == F.col("k.session_id")), "inner")
    .select("s.*")
)

result = (
    reconciled
    .groupBy("session_id", "event_type")
    .agg(
        F.count("*").alias("cnt"),
        F.min("event_time").alias("first_event_time"),
        F.countDistinct("event_type").alias("distinct_event_types"),
    )
    .withColumn("cnt_dense_rank", F.dense_rank().over(w))
    .orderBy("distinct_event_types")
)
