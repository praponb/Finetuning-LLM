from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_student_id = Window.partitionBy("student_id").orderBy("enrolled_at", "id")
w_student_id_running = Window.partitionBy("student_id").orderBy("enrolled_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_term_student_id = Window.partitionBy("term", "student_id")
w_grade_status_by_starts_sessi = Window.partitionBy("grade_status").orderBy(F.col("starts_session").desc(), "id")
w_term_by_prev_enrolled_at = Window.partitionBy("term").orderBy("prev_enrolled_at", "id")
w_term_by_prev_enrolled_at_run = Window.partitionBy("term").orderBy("prev_enrolled_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

filtered_enrollments = (
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

with_prev = filtered_enrollments.withColumn("prev_enrolled_at", F.lag("enrolled_at").over(w_student_id))

flagged = (
    with_prev
    .withColumn("starts_session", F.when(F.col("prev_enrolled_at").isNull() | ((F.unix_timestamp("enrolled_at") - F.unix_timestamp("prev_enrolled_at")) / 60 > 90), 1).otherwise(0))
)

sessionized = flagged.withColumn("burst_id", F.sum("starts_session").over(w_student_id_running))

with_trends = (
    sessionized
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "enrolled_at",
        "prev_enrolled_at",
        "burst_id",
        F.count(F.lit(1)).over(w_term_student_id).alias("rows_in_term_student_id"),
        F.ntile(10).over(w_grade_status_by_starts_sessi).alias("starts_session_decile"),
        F.round((F.unix_timestamp("prev_enrolled_at") - F.unix_timestamp(F.lag("prev_enrolled_at").over(w_term_by_prev_enrolled_at))) / 60, 1).alias("minutes_since_prev_prev_enrolled_at"),
        F.min("starts_session").over(w_term_student_id).alias("term_student_id_min_starts_session"),
        F.first("prev_enrolled_at", ignorenulls=True).over(w_term_by_prev_enrolled_at_run).alias("first_known_prev_enrolled_at"),
        (F.col("starts_session") - F.lag("starts_session").over(w_grade_status_by_starts_sessi)).alias("starts_session_change"),
    )
)

result = (
    with_trends
    .groupingSets(
        [["student_id", "term_student_id_min_starts_session"], ["student_id"], ["term_student_id_min_starts_session"], []],
        "student_id", "term_student_id_min_starts_session",
    )
    .agg(F.countDistinct("course_id").alias("distinct_course_ids"))
    .orderBy(
        F.col("student_id").asc_nulls_last(),
        F.col("term_student_id_min_starts_session").asc_nulls_last(),
    )
)
