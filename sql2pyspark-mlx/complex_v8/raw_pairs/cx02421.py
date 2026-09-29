from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_n_grade_status_incomp = Window.orderBy(F.col("n_grade_status_incomplete").asc_nulls_last(), "course_id", "student_id", "grade_status")
w_course_id_grade_status_by_n = Window.partitionBy("course_id", "grade_status").orderBy("n_grade_status_incomplete", "course_id", "student_id", "grade_status")
w_student_id_by_n_grade_status = Window.partitionBy("student_id").orderBy(F.col("n_grade_status_incomplete").desc(), "course_id", "student_id", "grade_status")
w_course_id_grade_status = Window.partitionBy("course_id", "grade_status")
w_student_cohort_row_count_by = Window.partitionBy("student_cohort", "row_count").orderBy("row_count", "course_id", "student_id", "grade_status")

recent_enrollments = (
    df_enrollments
    .filter(F.to_date(F.col("enrolled_at")) < F.lit("2025-02-06").cast("date"))
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
        "enrolled_at",
        F.coalesce(F.col("score"), F.lit(0.0)).alias("score_filled"),
        F.year(F.col("enrolled_at")).alias("enrolled_at_year"),
        F.concat_ws("|", F.col("grade_status"), F.col("term")).alias("grade_status_term_label"),
    )
)

annotated = (
    recent_enrollments
    .select(
        "id",
        "student_id",
        "course_id",
        "score",
        "grade_status",
        "score_filled",
        "grade_status_term_label",
        F.concat(F.col("grade_status_term_label"), F.lit("#"), F.col("enrolled_at_year").cast("string")).alias("grade_status_term_label_ref"),
        F.concat_ws("-", F.col("grade_status"), F.col("term")).alias("grade_status_term_label_2"),
        F.round(F.col("score") * 1.1, 2).alias("score_adj"),
    )
)

grouped = (
    annotated
    .filter(F.col("score") > 20.2)
    .groupBy("course_id", "student_id", "grade_status")
    .agg(
        F.count("*").alias("row_count"),
        F.min("score").alias("min_score"),
        F.count(F.when(F.col("grade_status") != "INCOMPLETE", 1)).alias("n_grade_status_incomplete"),
    )
)

ranked = (
    grouped
    .select(
        "*",
        F.ntile(5).over(w_all_by_n_grade_status_incomp).alias("n_grade_status_incomplete_quintile"),
        F.row_number().over(w_course_id_grade_status_by_n).alias("seq_no"),
        (F.col("n_grade_status_incomplete") - F.lag("n_grade_status_incomplete").over(w_student_id_by_n_grade_status)).alias("n_grade_status_incomplete_change"),
        (F.col("min_score") - F.lag("min_score").over(w_course_id_grade_status_by_n)).alias("min_score_change"),
    )
)

with_parent_course = (
    ranked.alias("s")
    .join(df_courses.alias("c"), (F.col("c.id") == F.col("s.course_id")) & (~F.col("c.level").isin("intro", "advanced")), "inner")
    .join(df_students.alias("st"), F.col("st.id") == F.col("s.student_id"), "left")
    .select(
        "s.course_id",
        "s.student_id",
        "s.grade_status",
        "s.row_count",
        "s.n_grade_status_incomplete",
        "s.seq_no",
        "s.n_grade_status_incomplete_change",
        "s.min_score_change",
        F.col("c.credits").alias("course_credits"),
        F.col("st.is_international").alias("student_is_international"),
        F.col("st.cohort").alias("student_cohort"),
    )
)

sequenced = (
    with_parent_course
    .select(
        "course_id",
        "student_id",
        "grade_status",
        "n_grade_status_incomplete",
        "course_credits",
        "student_is_international",
        "student_cohort",
        F.size(F.collect_set("student_id").over(w_course_id_grade_status)).alias("distinct_student_id_in_course_id_grade_status"),
        F.lag("row_count", 1, 0).over(w_student_cohort_row_count_by).alias("prev_row_count"),
    )
)

with_details_student = (
    sequenced.alias("s")
    .join(df_students.alias("st"), F.col("st.id") == F.col("s.student_id"), "inner")
    .join(df_courses.alias("c"), F.col("c.id") == F.col("s.course_id"), "left")
    .select(
        "s.course_id",
        "s.student_id",
        "s.grade_status",
        "s.n_grade_status_incomplete",
        "s.course_credits",
        "s.student_cohort",
        "s.prev_row_count",
        "st.student_name",
        F.col("st.gpa").alias("student_gpa"),
        F.col("st.major").alias("student_major"),
        F.col("c.level").alias("course_level"),
    )
)

result = (
    with_details_student
    .select(
        "course_id",
        "student_id",
        "grade_status",
        "n_grade_status_incomplete",
        "course_credits",
        "student_cohort",
        "prev_row_count",
        "student_name",
        "student_gpa",
        "course_level",
    )
    .orderBy(
        "student_id",
        F.col("student_name").asc_nulls_last(),
        "course_id",
        "grade_status",
    )
    .limit(20)
)
