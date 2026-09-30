from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_student_id = Window.partitionBy("student_id")
w_student_major_enrollment_gra = Window.partitionBy("student_major", "enrollment_grade_status").orderBy("score", "id", "enrollment_id").rowsBetween(-6, -1)
w_enrollment_term_term_by_enro = Window.partitionBy("enrollment_term", "term").orderBy(F.col("enrolled_at").asc_nulls_last(), "id", "enrollment_id")
w_enrollment_term_term_by_enro_2 = Window.partitionBy("enrollment_term", "term").orderBy(F.col("enrolled_at").asc_nulls_last(), "id", "enrollment_id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

base_enrollments = (
    df_enrollments
    .filter((F.col("enrolled_at") < F.lit("2025-02-18 11:00:00").cast("timestamp")) | (F.col("term") == "2025T2"))
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "enrolled_at",
        F.date_trunc("day", F.col("enrolled_at")).alias("enrolled_at_day"),
        F.when(F.col("score") >= 78.8, "A").when(F.col("score") >= 38.3, "B").otherwise("C").alias("score_tier"),
    )
)

detail_rows_enrollments = (
    base_enrollments.alias("s")
    .join(df_enrollments.alias("e"), F.col("e.course_id") == F.col("s.course_id"), "inner")
    .select(
        "s.id",
        "s.student_id",
        "s.course_id",
        "s.term",
        "s.score",
        "s.enrolled_at",
        "s.enrolled_at_day",
        "s.score_tier",
        F.col("e.id").alias("enrollment_id"),
        F.col("e.score").alias("enrollment_score"),
        F.col("e.grade_status").alias("enrollment_grade_status"),
        F.col("e.enrolled_at").alias("enrollment_enrolled_at"),
        F.col("e.term").alias("enrollment_term"),
    )
)

screened = (
    detail_rows_enrollments.alias("s")
    .join(df_enrollments.alias("e"), (F.col("e.course_id") == F.col("s.course_id")) & (F.col("e.grade_status") == "FAIL"), "left_semi")
    .filter(F.to_date(F.col("enrollment_enrolled_at")) < F.lit("2025-02-06").cast("date"))
)

combined_details_student = (
    screened.alias("s")
    .join(df_students.alias("st"), F.col("st.id") == F.col("s.student_id"), "inner")
    .select(
        "s.id",
        "s.student_id",
        "s.course_id",
        "s.term",
        "s.score",
        "s.enrolled_at",
        "s.score_tier",
        "s.enrollment_id",
        "s.enrollment_score",
        "s.enrollment_grade_status",
        "s.enrollment_enrolled_at",
        "s.enrollment_term",
        F.col("st.major").alias("student_major"),
    )
)

screened_2 = (
    combined_details_student.alias("s")
    .join(df_enrollments.alias("e"), (F.col("e.id") == F.col("s.enrollment_id")) & (F.col("e.term") == "2024T2") & (F.col("e.enrolled_at") >= F.lit("2025-01-31 09:00:00").cast("timestamp")), "left_semi")
)

with_windows = (
    screened_2
    .select(
        "*",
        F.size(F.collect_set("score_tier").over(w_student_id)).alias("distinct_score_tier_in_student_id"),
        F.round(F.avg("enrollment_score").over(w_student_major_enrollment_gra), 2).alias("enrollment_score_avg_6_prior"),
        (F.col("score") - F.lag("score").over(w_enrollment_term_term_by_enro)).alias("score_change"),
        F.count(F.lit(1)).over(w_enrollment_term_term_by_enro_2).alias("running_count"),
        F.round(F.col("score") - F.avg("score").over(w_student_id), 2).alias("score_vs_student_id_avg"),
    )
)

result = (
    with_windows
    .select(
        "id",
        "enrollment_id",
        "running_count",
        "distinct_score_tier_in_student_id",
    )
    .unpivot(
        ids=["id", "enrollment_id"],
        values=["running_count", "distinct_score_tier_in_student_id"],
        variableColumnName="field",
        valueColumnName="amount",
    )
    .orderBy("id", "enrollment_id", "field")
)
