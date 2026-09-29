from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("session_id").orderBy("duration_sec", "id")
w2 = Window.partitionBy("session_id").orderBy(F.col("event_time").cast("bigint")).rangeBetween(-7776000, Window.currentRow)

scored = (
    df_web_events
    .withColumn("next_duration_sec", F.lead("duration_sec").over(w))
    .withColumn("prev_duration_sec", F.lag("duration_sec").over(w))
    .withColumn("row_seq", F.row_number().over(w))
    .withColumn("duration_sec_avg_90d", F.avg("duration_sec").over(w2))
)

result = (
    scored
    .select(
        "id",
        "session_id",
        "event_type",
        "event_time",
        "duration_sec",
        "next_duration_sec",
        "prev_duration_sec",
        "row_seq",
        "duration_sec_avg_90d",
        F.when(F.col("event_type").rlike("^(search|purchase)$"), "CORE").when(F.col("event_type").rlike("^(page_view|click)$"), "GROUP_B").otherwise("EXTENDED").alias("event_type_zone"),
    )
    .orderBy(F.col("event_type").desc(), F.col("row_seq").desc())
)
