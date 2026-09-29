from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_running = Window.partitionBy("page", "session_id").orderBy("duration_sec", "id")
w_grp = Window.partitionBy(F.year("event_time"), "session_id").orderBy("duration_sec", "id")
w3 = Window.partitionBy("session_id").orderBy(F.col("next_duration_sec").desc_nulls_last(), "id")
w4 = Window.partitionBy("session_id").orderBy("row_seq", "id")
w5 = Window.orderBy(F.col("prev_duration_sec").desc())
w6 = Window.partitionBy("session_id").orderBy("duration_sec", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w7 = Window.partitionBy("session_id").orderBy(F.col("running_prev_duration_sec").desc(), "id")
w8 = Window.partitionBy("session_id").orderBy(F.col("next_duration_sec").asc_nulls_first(), "id")

enriched = (
    df_web_events
    .select(
        "id",
        "session_id",
        "duration_sec",
        F.lag("duration_sec").over(w_running).alias("prev_duration_sec"),
        F.lead("duration_sec").over(w_running).alias("next_duration_sec"),
        F.row_number().over(w_running).alias("row_seq"),
        F.lead("duration_sec", 1).over(w_grp).alias("next_duration_sec_2"),
    )
)

ranked = (
    enriched
    .withColumn("rn", F.row_number().over(w3))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

metrics = (
    ranked
    .filter(F.col("next_duration_sec") < 245)
    .select(
        "*",
        F.lead("next_duration_sec_2", 1).over(w4).alias("next_next_duration_sec_2"),
        F.percent_rank().over(w5).alias("prev_duration_sec_percent_rank"),
        F.sum("prev_duration_sec").over(w6).alias("running_prev_duration_sec"),
    )
)

deduped = (
    metrics
    .withColumn("rn", F.row_number().over(w7))
    .filter(F.col("rn") == 1)
)

deduped_web_events = (
    deduped
    .withColumn("rn_2", F.row_number().over(w8))
    .filter(F.col("rn_2") == 1)
    .select(
        "id",
        "duration_sec",
        "prev_duration_sec",
        "row_seq",
        "next_duration_sec_2",
        "next_next_duration_sec_2",
        "prev_duration_sec_percent_rank",
        "running_prev_duration_sec",
    )
)

result = (
    deduped_web_events
    .select(
        "id",
        "prev_duration_sec",
        "row_seq",
        "next_next_duration_sec_2",
        "prev_duration_sec_percent_rank",
        "running_prev_duration_sec",
    )
    .orderBy(F.col("id").desc_nulls_last(), "prev_duration_sec_percent_rank")
)
