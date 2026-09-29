from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_term = Window.partitionBy("term").orderBy("enrolled_at", "id")
w_term_running = Window.partitionBy("term").orderBy("enrolled_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_events_ties = Window.orderBy(F.col("events").asc_nulls_last())
w_all_by_events = Window.orderBy(F.col("events").asc_nulls_last(), "term", "burst_id")
w_term_burst_id = Window.partitionBy("term", "burst_id")
w_burst_id = Window.partitionBy("burst_id")

recent = (
    df_enrollments
    .filter(F.hour(F.col("enrolled_at")).between(8, 14))
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "enrolled_at",
        F.date_format(F.col("enrolled_at"), "yyyy-MM-dd HH:00").alias("enrolled_at_label"),
    )
)

with_prev = recent.withColumn("prev_enrolled_at", F.lag("enrolled_at").over(w_term))

flagged = (
    with_prev
    .withColumn("is_new_session", F.when(F.col("prev_enrolled_at").isNull() | ((F.unix_timestamp("enrolled_at") - F.unix_timestamp("prev_enrolled_at")) / 60 > 90), 1).otherwise(0))
)

sessionized = flagged.withColumn("burst_id", F.sum("is_new_session").over(w_term_running))

per_session = (
    sessionized
    .groupBy("term", "burst_id")
    .agg(
        F.min("enrolled_at").alias("session_start"),
        F.max("enrolled_at").alias("session_end"),
        F.count("*").alias("events"),
        F.round((F.unix_timestamp(F.max("enrolled_at")) - F.unix_timestamp(F.min("enrolled_at"))) / 60, 1).alias("duration_min"),
    )
)

with_trends = (
    per_session
    .select(
        "*",
        F.dense_rank().over(w_all_by_events_ties).alias("events_dense_rank"),
        F.lag("events", 1, 0).over(w_all_by_events).alias("prev_events"),
        F.round(F.avg("burst_id").over(w_term_burst_id), 2).alias("term_burst_id_avg_burst_id"),
        F.dense_rank().over(w_all_by_events).alias("events_dense_rank_2"),
        F.max("duration_min").over(w_term_burst_id).alias("term_burst_id_max_duration_min"),
    )
)

sequenced = (
    with_trends
    .select(
        "term",
        "burst_id",
        "session_start",
        "session_end",
        "term_burst_id_avg_burst_id",
        "events_dense_rank_2",
        "term_burst_id_max_duration_min",
        F.max("burst_id").over(w_burst_id).alias("burst_id_max_burst_id"),
        F.round(F.avg("events_dense_rank").over(w_burst_id), 2).alias("burst_id_avg_events_dense_rank"),
        F.round(F.col("events_dense_rank_2") / F.nullif(F.sum("events_dense_rank_2").over(w_burst_id), F.lit(0)), 4).alias("events_dense_rank_2_share_of_burst_id"),
    )
)

result = (
    sequenced
    .select("burst_id", "term", "session_end")
    .distinct()
    .orderBy(F.col("burst_id").desc(), F.col("session_end").desc())
)
