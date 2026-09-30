from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_course_id_term_by_score_minu = Window.partitionBy("course_id", "term").orderBy(F.col("score_minus_max").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_course_id_by_student_gpa = Window.partitionBy("course_id").orderBy(F.col("student_gpa").asc_nulls_last(), "id")
w_term = Window.partitionBy("term")
w_term_course_id_by_max_score = Window.partitionBy("term", "course_id").orderBy(F.col("max_score").desc_nulls_first(), "id")

valid_enrollments = (
    df_enrollments
    .filter((F.col("term").isin("2025T1", "2025T2", "2024T1")) & (F.col("enrolled_at") >= F.lit("2025-02-06 11:00:00").cast("timestamp")))
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
        "enrolled_at",
        F.datediff(F.lit("2025-12-31").cast("date"), F.col("enrolled_at")).alias("days_since_enrolled_at"),
        F.coalesce(F.col("term"), F.lit("UNKNOWN")).alias("term_or_unknown"),
    )
)

with_details_student = (
    valid_enrollments.alias("s")
    .join(df_students.alias("st"), F.col("st.id") == F.col("s.student_id"), "inner")
    .select(
        "s.id",
        "s.student_id",
        "s.course_id",
        "s.term",
        "s.score",
        "s.enrolled_at",
        "s.days_since_enrolled_at",
        "s.term_or_unknown",
        F.col("st.gpa").alias("student_gpa"),
    )
)

student_id_stats = (
    with_details_student
    .groupBy("student_id")
    .agg(F.max("score").alias("max_score"))
)

compared = (
    with_details_student.alias("s")
    .join(student_id_stats.alias("g"), F.col("g.student_id") == F.col("s.student_id"), "inner")
    .filter(F.col("s.score") == F.col("g.max_score"))
    .select(
        "s.*",
        "g.max_score",
        F.round(F.col("s.score") - F.col("g.max_score"), 2).alias("score_minus_max"),
    )
)

windowed = (
    compared
    .select(
        "*",
        F.last("score", ignorenulls=True).over(w_course_id_term_by_score_minu).alias("last_known_score"),
        (F.col("student_gpa") - F.lag("student_gpa").over(w_course_id_by_student_gpa)).alias("student_gpa_change"),
        F.min("max_score").over(w_term).alias("term_min_max_score"),
        F.round(F.cume_dist().over(w_term_course_id_by_max_score), 3).alias("max_score_cume_dist"),
    )
)

enrollment_stats = (
    df_enrollments
    .groupBy("student_id")
    .agg(F.max("enrolled_at").alias("last_enrollment_enrolled_at"))
)

summarized_rows = (
    windowed.alias("s")
    .join(enrollment_stats.alias("e"), F.col("e.student_id") == F.col("s.student_id"), "left")
    .select("s.*", "e.last_enrollment_enrolled_at")
)

result = (
    summarized_rows
    .select(
        "id",
        "course_id",
        "score",
        "enrolled_at",
        "term_or_unknown",
        "max_score",
        "score_minus_max",
        "student_gpa_change",
        "max_score_cume_dist",
        "last_enrollment_enrolled_at",
        F.coalesce(F.col("max_score_cume_dist"), F.lit(0.0)).alias("max_score_cume_dist_filled"),
        F.when(F.col("term_or_unknown").isin("2024T1", "2025T1"), "group_a").when(F.col("term_or_unknown") == "2025T2", "group_b").otherwise("other").alias("term_or_unknown_group"),
    )
    .orderBy(
        "student_gpa_change",
        F.col("last_enrollment_enrolled_at").desc_nulls_first(),
    )
)
