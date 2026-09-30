from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_student_id_course_id_by_enro = Window.partitionBy("student_id", "course_id").orderBy("enrolled_at_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_score_moving_3 = Window.orderBy("score", "id").rowsBetween(-3, Window.currentRow)
w_course_id = Window.partitionBy("course_id")
w_all_by_score_full = Window.orderBy("score", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_student_id_course_id_by_enro_2 = Window.partitionBy("student_id", "course_id").orderBy("enrolled_at_date", "id").rowsBetween(-3, Window.currentRow)

scoped_enrollments = (
    df_enrollments
    .filter(F.to_date(F.col("enrolled_at")) < F.lit("2025-03-08").cast("date"))
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
        "enrolled_at",
        F.coalesce(F.col("score"), F.lit(0.0)).alias("score_filled"),
        F.round(F.col("score") * 0.25, 2).alias("score_adj"),
        F.to_date(F.col("enrolled_at")).alias("enrolled_at_date"),
    )
)

sequenced = (
    scoped_enrollments
    .select(
        "*",
        F.first("enrolled_at", ignorenulls=True).over(w_student_id_course_id_by_enro).alias("first_known_enrolled_at"),
        F.sum("score_adj").over(w_all_by_score_moving_3).alias("score_adj_sum_4"),
        F.round(F.avg("score_adj").over(w_course_id), 2).alias("course_id_avg_score_adj"),
        F.last("score").over(w_all_by_score_full).alias("last_score"),
        F.round(F.col("score_filled") - F.avg("score_filled").over(w_course_id), 2).alias("score_filled_vs_course_id_avg"),
        F.max("score_filled").over(w_student_id_course_id_by_enro_2).alias("score_filled_max_4"),
    )
)

enrollment_stats = (
    df_enrollments
    .groupBy("student_id")
    .agg(
        F.min("score").alias("min_enrollment_score"),
        F.min("score").alias("min_enrollment_score_2"),
        F.sum("score").alias("total_enrollment_score"),
    )
)

with_counts = (
    sequenced.alias("s")
    .join(enrollment_stats.alias("e"), F.col("e.student_id") == F.col("s.student_id"), "left")
    .select(
        "s.*",
        "e.min_enrollment_score",
        "e.min_enrollment_score_2",
        "e.total_enrollment_score",
    )
)

tagged = (
    with_counts
    .filter(F.col("min_enrollment_score_2") < 80.0)
    .select(
        "*",
        F.coalesce(F.col("term"), F.lit("UNKNOWN")).alias("term_or_unknown"),
        F.length(F.trim(F.col("grade_status"))).alias("grade_status_len"),
    )
)

with_activity = (
    tagged.alias("s")
    .join(df_enrollments.alias("e"), (F.col("e.student_id") == F.col("s.student_id")) & (F.col("e.term") != "2025T1") & (F.col("e.course_id").isNotNull()), "left_anti")
    .filter(F.col("score") < 80.2)
)

screened = (
    with_activity.alias("s")
    .join(df_courses.alias("c"), (F.col("c.id") == F.col("s.course_id")) & (F.col("c.dept").isin("CS", "Math", "Bio")), "left_semi")
)

with_parent = (
    screened.alias("s")
    .join(df_courses.alias("c"), F.col("c.id") == F.col("s.course_id"), "inner")
    .join(df_students.alias("st"), F.col("st.id") == F.col("s.student_id"), "inner")
    .select(
        "s.id",
        "s.student_id",
        "s.course_id",
        "s.term",
        "s.score",
        "s.enrolled_at",
        "s.score_filled",
        "s.score_adj",
        "s.enrolled_at_date",
        "s.score_adj_sum_4",
        "s.course_id_avg_score_adj",
        "s.score_filled_max_4",
        "s.min_enrollment_score",
        "s.total_enrollment_score",
        "s.term_or_unknown",
        "s.grade_status_len",
        "c.course_code",
        F.col("c.credits").alias("course_credits"),
        F.col("c.level").alias("course_level"),
        "st.student_name",
        F.col("st.enrolled_on").alias("student_enrolled_on"),
    )
)

result = (
    with_parent
    .select(
        "id",
        "term",
        "score",
        "enrolled_at",
        "score_filled",
        "score_adj",
        "enrolled_at_date",
        "course_id_avg_score_adj",
        "score_filled_max_4",
        "min_enrollment_score",
        "total_enrollment_score",
        "grade_status_len",
        "course_code",
        "course_level",
        "student_enrolled_on",
        F.when(F.col("grade_status_len") >= 12, "A").when(F.col("grade_status_len") >= 6, "B").otherwise("C").alias("grade_status_len_tier"),
    )
)
