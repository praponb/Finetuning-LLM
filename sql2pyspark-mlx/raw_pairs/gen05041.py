from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("event_type").orderBy("event_time", "id")
w2 = Window.partitionBy("event_type").orderBy("duration_sec", "id")
w3 = Window.partitionBy("session_id").orderBy(F.col("duration_sec").desc(), "id")
duration_sec_by_session_id = df_web_events.groupBy("session_id").agg(F.sum("duration_sec").alias("sum_web_event_duration_sec"))

metrics = (
    df_web_events
    .withColumn("event_seq", F.row_number().over(w))
    .withColumn("event_seq_2", F.row_number().over(w2))
    .withColumn("duration_sec_bucket", F.ntile(4).over(w3))
)

result = (
    metrics
    .join(duration_sec_by_session_id, on="session_id", how="left")
    .select(
        "id",
        "session_id",
        "event_type",
        "event_time",
        "duration_sec",
        "event_seq",
        "event_seq_2",
        "duration_sec_bucket",
        F.when(F.col("event_type").rlike("^(page_view|purchase)$"), "CORE").otherwise("GROUP_B").alias("event_type_zone"),
        F.col("sum_web_event_duration_sec"),
    )
    .orderBy(F.col("event_seq_2").desc())
)
