from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_grade_status_term = Window.partitionBy("grade_status", "term")
w_grade_status_course_id_by_en = Window.partitionBy("grade_status", "course_id").orderBy("enrolled_at_quarter", "id")
w_course_id_student_id_by_enro = Window.partitionBy("course_id", "student_id").orderBy(F.col("enrolled_at").desc(), "id")

scoped = (
    df_enrollments
    .filter((F.to_date(F.col("enrolled_at")) < F.lit("2025-02-24").cast("date")) & (F.col("term") == "2024T1"))
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
        "enrolled_at",
        F.round(F.col("score") * 1.07, 2).alias("score_adj"),
        F.quarter(F.col("enrolled_at")).alias("enrolled_at_quarter"),
    )
)

windowed = (
    scoped
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
        "enrolled_at",
        "score_adj",
        "enrolled_at_quarter",
        F.count(F.lit(1)).over(w_grade_status_term).alias("rows_in_grade_status_term"),
        F.round(F.avg("score").over(w_grade_status_term), 2).alias("grade_status_term_avg_score"),
        F.ntile(10).over(w_grade_status_course_id_by_en).alias("enrolled_at_quarter_decile"),
        F.lag("score_adj").over(w_course_id_student_id_by_enro).alias("prev_score_adj"),
    )
)

enriched_student = (
    windowed.alias("s")
    .join(df_students.alias("st"), F.col("st.id") == F.col("s.student_id"), "inner")
    .filter(~F.col("st.is_international"))
    .select(
        "s.id",
        "s.student_id",
        "s.course_id",
        "s.term",
        "s.score",
        "s.grade_status",
        "s.enrolled_at",
        "s.score_adj",
        "s.enrolled_at_quarter",
        "s.rows_in_grade_status_term",
        "s.grade_status_term_avg_score",
        "s.prev_score_adj",
        F.col("st.gpa").alias("student_gpa"),
        F.col("st.enrolled_on").alias("student_enrolled_on"),
        F.col("st.is_international").alias("student_is_international"),
    )
)

result = (
    enriched_student
    .cube("course_id", "student_id")
    .agg(
        F.round(F.sum("score_adj") / F.nullif(F.sum("student_gpa"), F.lit(0)), 3).alias("score_adj_to_student_gpa_ratio"),
    )
    .orderBy(
        F.col("course_id").asc_nulls_last(),
        F.col("student_id").asc_nulls_last(),
    )
)
