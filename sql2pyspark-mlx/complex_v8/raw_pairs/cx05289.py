from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_student_id = Window.partitionBy("student_id").orderBy("enrolled_at", "id")
w_student_id_running = Window.partitionBy("student_id").orderBy("enrolled_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_events_ties = Window.orderBy(F.col("events").desc_nulls_first())
w_all_by_events_running = Window.orderBy(F.col("events").desc_nulls_first(), "student_id", "burst_id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_student_id_2 = Window.partitionBy("student_id")
w_all_by_events = Window.orderBy(F.col("events").desc_nulls_first(), "student_id", "burst_id")
w_all_by_events_prior_6 = Window.orderBy(F.col("events").desc_nulls_first(), "student_id", "burst_id").rowsBetween(-6, -1)

cleaned = (
    df_enrollments
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
        "enrolled_at",
    )
)

with_prev = cleaned.withColumn("prev_enrolled_at", F.lag("enrolled_at").over(w_student_id))

flagged = (
    with_prev
    .withColumn("starts_session", F.when(F.col("prev_enrolled_at").isNull() | ((F.unix_timestamp("enrolled_at") - F.unix_timestamp("prev_enrolled_at")) / 60 > 30), 1).otherwise(0))
)

sessionized = flagged.withColumn("burst_id", F.sum("starts_session").over(w_student_id_running))

sessions = (
    sessionized
    .groupBy("student_id", "burst_id")
    .agg(
        F.min("enrolled_at").alias("session_start"),
        F.max("enrolled_at").alias("session_end"),
        F.count("*").alias("events"),
        F.sum("score").alias("session_score"),
        F.round((F.unix_timestamp(F.max("enrolled_at")) - F.unix_timestamp(F.min("enrolled_at"))) / 60, 1).alias("duration_min"),
    )
)

with_trends = (
    sessions
    .select(
        "student_id",
        "burst_id",
        "session_score",
        F.dense_rank().over(w_all_by_events_ties).alias("events_dense_rank"),
        F.min("burst_id").over(w_all_by_events_running).alias("min_burst_id_so_far"),
        F.sum("duration_min").over(w_student_id_2).alias("student_id_sum_duration_min"),
        F.round((F.unix_timestamp("session_start") - F.unix_timestamp(F.lag("session_start").over(w_all_by_events))) / 60, 1).alias("minutes_since_prev_session_start"),
        F.round(F.cume_dist().over(w_all_by_events_ties), 3).alias("events_cume_dist"),
        F.sum("duration_min").over(w_all_by_events_prior_6).alias("duration_min_sum_6_prior"),
    )
)

derived = (
    with_trends
    .select(
        "student_id",
        "burst_id",
        "session_score",
        "events_dense_rank",
        "student_id_sum_duration_min",
        "minutes_since_prev_session_start",
        "events_cume_dist",
        "duration_min_sum_6_prior",
        F.abs(F.col("events_dense_rank") - 5).alias("events_dense_rank_distance"),
        F.round(F.col("events_dense_rank") / F.nullif(F.col("events_cume_dist"), F.lit(0)), 3).alias("events_dense_rank_per_events_cume_dist"),
    )
)

result = (
    derived
    .filter(F.col("events_dense_rank_distance") >= 4)
    .select(
        "student_id",
        "burst_id",
        "session_score",
        "events_dense_rank",
        "student_id_sum_duration_min",
        "minutes_since_prev_session_start",
        "events_cume_dist",
        "duration_min_sum_6_prior",
        "events_dense_rank_distance",
        "events_dense_rank_per_events_cume_dist",
        F.round(F.col("events_cume_dist") / F.nullif(F.col("burst_id"), F.lit(0)), 3).alias("events_cume_dist_per_burst_id"),
        F.round(F.col("burst_id") / F.nullif(F.col("events_dense_rank"), F.lit(0)), 3).alias("burst_id_per_events_dense_rank"),
    )
    .orderBy(F.col("events_dense_rank_per_events_cume_dist").desc_nulls_first())
)
