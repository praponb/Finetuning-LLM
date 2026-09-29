from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("session_id").orderBy("event_time", "id")
w2 = Window.partitionBy("session_id")
w3 = Window.orderBy("row_seq", "id")
w4 = Window.orderBy("session_id_max_duration_sec", "id")
w5 = Window.orderBy("row_seq", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.orderBy("prev_duration_sec", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w7 = Window.orderBy("session_id_max_duration_sec_plus_row_seq", "id")
w8 = Window.orderBy("cumulative_session_id_max_duration_sec_plus_row_seq", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w9 = Window.orderBy("session_id_max_duration_sec", "id").rowsBetween(-6, Window.currentRow)

metrics = (
    df_web_events
    .select(
        "id",
        "duration_sec",
        F.lag("duration_sec").over(w).alias("prev_duration_sec"),
        F.row_number().over(w).alias("row_seq"),
        F.max("duration_sec").over(w2).alias("session_id_max_duration_sec"),
    )
)

base = (
    metrics
    .filter(F.col("session_id_max_duration_sec").between(198, 291))
    .select(
        "*",
        (F.col("session_id_max_duration_sec") + F.col("row_seq")).alias("session_id_max_duration_sec_plus_row_seq"),
    )
)

with_windows = (
    base
    .select(
        "id",
        "duration_sec",
        "row_seq",
        "session_id_max_duration_sec",
        "session_id_max_duration_sec_plus_row_seq",
        F.lag("duration_sec", 2, 0).over(w3).alias("prev_duration_sec_2"),
        F.lead("prev_duration_sec", 1).over(w4).alias("next_prev_duration_sec"),
        F.sum("session_id_max_duration_sec_plus_row_seq").over(w5).alias("cumulative_session_id_max_duration_sec_plus_row_seq"),
        F.sum("row_seq").over(w6).alias("running_row_seq"),
        F.lead("session_id_max_duration_sec", 2, 0).over(w7).alias("next_session_id_max_duration_sec"),
    )
)

enriched = (
    with_windows
    .withColumn("last_known_duration_sec", F.last("duration_sec", ignorenulls=True).over(w8))
    .withColumn("duration_sec_avg_6", F.avg("duration_sec").over(w9))
)

totals = (
    enriched
    .withColumn("prev_duration_sec_2_tier", F.when(F.col("prev_duration_sec_2") >= 513, "PREMIUM").otherwise("STANDARD"))
    .withColumn("next_session_id_max_duration_sec_tier", F.when(F.col("next_session_id_max_duration_sec") >= 477, "PREMIUM").otherwise("STANDARD"))
    .groupingSets(
        [["prev_duration_sec_2_tier", "next_session_id_max_duration_sec_tier"], ["prev_duration_sec_2_tier"], []],
        "prev_duration_sec_2_tier",
        "next_session_id_max_duration_sec_tier",
    )
    .agg(
        F.count("*").alias("num_rows"),
        F.round(F.avg("cumulative_session_id_max_duration_sec_plus_row_seq"), 2).alias("avg_cumulative_session_id_max_duration_sec_plus_row_seq"),
        F.sum("row_seq").alias("total_row_seq"),
        F.countDistinct("id").alias("distinct_ids"),
        F.grouping_id().alias("grouping_level"),
    )
)

result = (
    totals
    .select(
        "next_session_id_max_duration_sec_tier",
        "avg_cumulative_session_id_max_duration_sec_plus_row_seq",
        "total_row_seq",
        "distinct_ids",
        "grouping_level",
    )
)
