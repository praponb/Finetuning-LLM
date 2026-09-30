from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_enrollment_id_grade_status_o = Window.partitionBy("enrollment_id", "grade_status_or_unknown").orderBy(F.col("avg_enrollment_score").desc(), "id", "enrollment_id")
w_all_by_enrollment_score_runn = Window.orderBy("enrollment_score", "id", "enrollment_id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_enrollment_id = Window.partitionBy("enrollment_id")
w_enrollment_id_grade_status_o_2 = Window.partitionBy("enrollment_id", "grade_status_or_unknown").orderBy(F.col("avg_enrollment_score").desc(), "id", "enrollment_id").rowsBetween(-6, Window.currentRow)

scoped_enrollments = (
    df_enrollments
    .filter((F.col("enrolled_at") >= F.lit("2025-01-19 15:00:00").cast("timestamp")) & (F.col("student_id") >= 8))
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "grade_status",
        "enrolled_at",
    )
)

with_details_student = (
    scoped_enrollments.alias("s")
    .join(df_students.alias("st"), F.col("st.id") == F.col("s.student_id"), "left")
    .select(
        "s.id",
        "s.student_id",
        "s.course_id",
        "s.term",
        "s.grade_status",
        "s.enrolled_at",
        F.col("st.is_international").alias("student_is_international"),
    )
)

enriched = (
    with_details_student
    .select(
        "*",
        F.coalesce(F.col("grade_status"), F.lit("UNKNOWN")).alias("grade_status_or_unknown"),
        F.to_date(F.col("enrolled_at")).alias("enrolled_at_date"),
    )
)

enrollment_stats = (
    df_enrollments
    .groupBy("student_id")
    .agg(
        F.round(F.avg("score"), 2).alias("avg_enrollment_score"),
        F.max("enrolled_at").alias("last_enrollment_enrolled_at"),
    )
)

with_stats = (
    enriched.alias("s")
    .join(enrollment_stats.alias("e"), F.col("e.student_id") == F.col("s.student_id"), "left")
    .select("s.*", "e.avg_enrollment_score", "e.last_enrollment_enrolled_at")
)

with_children = (
    with_stats.alias("s")
    .join(df_enrollments.alias("e"), (F.col("e.student_id") == F.col("s.student_id")) & (F.col("e.score") >= 17.4), "inner")
    .select(
        "s.id",
        "s.student_id",
        "s.course_id",
        "s.term",
        "s.enrolled_at",
        "s.student_is_international",
        "s.grade_status_or_unknown",
        "s.avg_enrollment_score",
        F.col("e.id").alias("enrollment_id"),
        F.col("e.score").alias("enrollment_score"),
        F.col("e.term").alias("enrollment_term"),
        F.col("e.enrolled_at").alias("enrollment_enrolled_at"),
    )
)

sequenced = (
    with_children
    .select(
        "id",
        "student_id",
        "course_id",
        "grade_status_or_unknown",
        "enrollment_id",
        "enrollment_term",
        "enrollment_enrolled_at",
        F.lead("enrollment_enrolled_at").over(w_enrollment_id_grade_status_o).alias("next_enrollment_enrolled_at"),
        F.min("avg_enrollment_score").over(w_all_by_enrollment_score_runn).alias("min_avg_enrollment_score_so_far"),
        F.count(F.lit(1)).over(w_enrollment_id).alias("rows_in_enrollment_id"),
        F.round(F.avg("avg_enrollment_score").over(w_enrollment_id_grade_status_o_2), 2).alias("avg_enrollment_score_avg_7"),
        F.size(F.collect_set("course_id").over(w_enrollment_id)).alias("distinct_course_id_in_enrollment_id"),
    )
)

with_parent_student = (
    sequenced.alias("s")
    .join(df_students.alias("st"), F.col("st.id") == F.col("s.student_id"), "inner")
    .select(
        "s.*",
        F.col("st.gpa").alias("student_gpa"),
        F.col("st.is_international").alias("student_is_international"),
    )
)

result = (
    with_parent_student
    .groupBy("enrollment_term", "student_id")
    .pivot("grade_status_or_unknown", ["WITHDRAWN", "INCOMPLETE"])
    .agg(F.count("avg_enrollment_score_avg_7"))
)
