from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_term = Window.partitionBy("term").orderBy("enrolled_at", "id")
w_term_running = Window.partitionBy("term").orderBy("enrolled_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_session_end_month_n_events_b = Window.partitionBy(F.date_trunc("month", "session_end"), "n_events").orderBy("session_score", "term", "session_id")
w_session_end_month_n_events_b_2 = Window.partitionBy(F.date_trunc("month", "session_end"), "n_events").orderBy("session_score", "term", "session_id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_session_id_n_events_by_sessi = Window.partitionBy("session_id", "n_events").orderBy(F.col("session_start").asc_nulls_last(), "term", "session_id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_session_id_n_events_by_sessi_2 = Window.partitionBy("session_id", "n_events").orderBy(F.col("session_start").asc_nulls_last())

cleaned_enrollments = (
    df_enrollments
    .filter(F.col("student_id").isNotNull())
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
        "enrolled_at",
        F.concat_ws(" / ", F.col("grade_status"), F.col("term")).alias("grade_status_term_label"),
    )
)

with_prev = cleaned_enrollments.withColumn("prev_enrolled_at", F.lag("enrolled_at").over(w_term))

flagged = (
    with_prev
    .withColumn("new_session_flag", F.when(F.col("prev_enrolled_at").isNull() | (F.col("enrolled_at") > F.col("prev_enrolled_at") + F.expr("INTERVAL 45 MINUTES")), 1).otherwise(0))
)

sessionized = flagged.withColumn("session_id", F.sum("new_session_flag").over(w_term_running))

sessions = (
    sessionized
    .groupBy("term", "session_id")
    .agg(
        F.min("enrolled_at").alias("session_start"),
        F.max("enrolled_at").alias("session_end"),
        F.count("*").alias("n_events"),
        F.sum("score").alias("session_score"),
        F.round((F.unix_timestamp(F.max("enrolled_at")) - F.unix_timestamp(F.min("enrolled_at"))) / 60, 1).alias("duration_min"),
    )
)

sequenced = (
    sessions
    .select(
        "term",
        "session_id",
        "session_start",
        "session_end",
        "n_events",
        "duration_min",
        F.dense_rank().over(w_session_end_month_n_events_b).alias("session_score_dense_rank"),
        F.last("duration_min", ignorenulls=True).over(w_session_end_month_n_events_b_2).alias("last_known_duration_min"),
        F.max("n_events").over(w_session_id_n_events_by_sessi).alias("max_n_events_so_far"),
        F.round(F.cume_dist().over(w_session_id_n_events_by_sessi_2), 3).alias("session_start_cume_dist"),
    )
)

result = (
    sequenced
    .select(
        "term",
        "session_id",
        F.col("last_known_duration_min").alias("last_known_duration_min_v"),
        F.col("session_start_cume_dist").alias("session_start_cume_dist_v"),
        F.col("duration_min").alias("duration_min_v"),
    )
    .unpivot(
        ids=["term", "session_id"],
        values=["last_known_duration_min_v", "session_start_cume_dist_v", "duration_min_v"],
        variableColumnName="metric",
        valueColumnName="reading",
    )
    .filter(F.col("reading").isNotNull())
    .orderBy("term", "session_id", "metric")
)
