from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_course_id = Window.partitionBy("course_id").orderBy("enrolled_at", "id")
w_course_id_running = Window.partitionBy("course_id").orderBy("enrolled_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_course_id_session_id = Window.partitionBy("course_id", "session_id")
w_session_end_to_date_by_sessi = Window.partitionBy(F.to_date("session_end")).orderBy("session_start", "course_id", "session_id")
w_session_start_month_course_i = Window.partitionBy(F.date_trunc("month", "session_start"), "course_id").orderBy(F.col("session_end").desc(), "course_id", "session_id")

base_enrollments = (
    df_enrollments
    .filter(F.col("grade_status").isNotNull())
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
        "enrolled_at",
        F.when(F.col("score") >= 81.4, "HIGH").when(F.col("score") >= 38.3, "MEDIUM").otherwise("LOW").alias("score_tier"),
    )
)

with_prev = base_enrollments.withColumn("prev_enrolled_at", F.lag("enrolled_at").over(w_course_id))

flagged = (
    with_prev
    .withColumn("new_session_flag", F.when(F.col("prev_enrolled_at").isNull() | (F.col("enrolled_at") > F.col("prev_enrolled_at") + F.expr("INTERVAL 30 MINUTES")), 1).otherwise(0))
)

sessionized = flagged.withColumn("session_id", F.sum("new_session_flag").over(w_course_id_running))

sessions = (
    sessionized
    .groupBy("course_id", "session_id")
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
        "*",
        F.sum("session_id").over(w_course_id_session_id).alias("course_id_session_id_sum_session_id"),
        F.count(F.lit(1)).over(w_course_id_session_id).alias("rows_in_course_id_session_id"),
        F.lead("n_events").over(w_session_end_to_date_by_sessi).alias("next_n_events"),
        F.sum("session_score").over(w_course_id_session_id).alias("course_id_session_id_sum_session_score"),
        F.lag("session_end", 2).over(w_session_start_month_course_i).alias("prev2_session_end"),
    )
)

enrollment_stats = (
    df_enrollments
    .groupBy("course_id")
    .agg(
        F.count("*").alias("enrollment_count"),
        F.min("enrolled_at").alias("first_enrollment_enrolled_at"),
        F.min("enrolled_at").alias("first_enrollment_enrolled_at_2"),
    )
)

summarized_rows = (
    sequenced.alias("s")
    .join(enrollment_stats.alias("e"), F.col("e.course_id") == F.col("s.course_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("e.enrollment_count"), F.lit(0)).alias("enrollment_count"),
        "e.first_enrollment_enrolled_at",
        "e.first_enrollment_enrolled_at_2",
    )
)

result = (
    summarized_rows
    .select(
        "course_id",
        "session_start",
        "duration_min",
        "course_id_session_id_sum_session_id",
        "rows_in_course_id_session_id",
        "next_n_events",
        "course_id_session_id_sum_session_score",
        "enrollment_count",
        "first_enrollment_enrolled_at_2",
        F.to_date(F.col("session_end")).alias("session_end_date"),
    )
    .orderBy(F.col("enrollment_count").asc())
)
