from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("page").orderBy("event_time", "id").rowsBetween(-5, Window.currentRow)
w2 = Window.orderBy("duration_sec", "id")
w_recent = Window.partitionBy("event_type").orderBy("duration_sec", "id")
w_running = Window.partitionBy("session_id").orderBy("duration_sec", "id").rowsBetween(-6, -1)
w5 = Window.orderBy("duration_sec", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.partitionBy("session_id").orderBy(F.col("row_seq").asc_nulls_first(), "id")
w7 = Window.partitionBy("page").orderBy(F.col("duration_sec").desc(), "id")

scored = (
    df_web_events
    .withColumn("duration_sec_avg_5", F.avg("duration_sec").over(w))
    .withColumn("seq_no", F.row_number().over(w2))
    .withColumn("row_seq", F.row_number().over(w_recent))
    .withColumn("duration_sec_avg_6_prior", F.avg("duration_sec").over(w_running))
    .withColumn("first_known_duration_sec", F.first("duration_sec", ignorenulls=True).over(w5))
)

deduped = (
    scored
    .withColumn("rn", F.row_number().over(w6))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

deduped_web_events = (
    deduped
    .withColumn("rank_in_group", F.row_number().over(w7))
    .filter(F.col("rank_in_group") == 1)
)

with_dims = (
    deduped_web_events
    .alias("e")
    .join(df_sessions.alias("d"), F.col("e.session_id") == F.col("d.id"), "inner")
    .select("e.*", F.col("d.device"))
)

result = (
    with_dims
    .select(
        "id",
        "event_type",
        "page",
        "event_time",
        "duration_sec",
        "seq_no",
        "row_seq",
        "first_known_duration_sec",
        "rank_in_group",
    )
    .orderBy("event_type")
)
