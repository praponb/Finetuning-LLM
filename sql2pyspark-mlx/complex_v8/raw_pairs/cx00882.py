from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_student_id_term = Window.partitionBy("student_id", "term")
w_term_by_enrolled_at_full = Window.partitionBy("term").orderBy("enrolled_at", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

prepared_enrollments = (
    df_enrollments
    .filter((~F.col("term").isin("2025T2", "2024T2")) | (F.month(F.col("enrolled_at")).isin(2, 3)))
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "enrolled_at",
        F.concat_ws(": ", F.col("grade_status"), F.col("term")).alias("grade_status_term_label"),
        F.length(F.trim(F.col("grade_status"))).alias("grade_status_len"),
        F.initcap(F.trim(F.col("term"))).alias("term_clean"),
    )
)

with_trends = (
    prepared_enrollments
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "enrolled_at",
        F.round(F.col("grade_status_len") / F.nullif(F.sum("grade_status_len").over(w_student_id_term), F.lit(0)), 4).alias("grade_status_len_share_of_student_id_term"),
        F.first("score").over(w_term_by_enrolled_at_full).alias("first_score"),
        F.sum("grade_status_len").over(w_student_id_term).alias("student_id_term_sum_grade_status_len"),
        F.count(F.lit(1)).over(w_student_id_term).alias("rows_in_student_id_term"),
        F.round(F.col("score") / F.nullif(F.sum("score").over(w_student_id_term), F.lit(0)), 4).alias("score_share_of_student_id_term"),
        F.nth_value("enrolled_at", 2).over(w_term_by_enrolled_at_full).alias("second_enrolled_at"),
    )
)

enriched = (
    with_trends.alias("s")
    .join(df_courses.alias("c"), (F.col("c.id") == F.col("s.course_id")) & (F.col("c.level").isin("advanced", "intro")), "inner")
    .filter(~F.col("c.level").isin("core", "advanced"))
    .select(
        "s.id",
        "s.student_id",
        "s.course_id",
        "s.term",
        "s.grade_status_len_share_of_student_id_term",
        "s.first_score",
        "s.student_id_term_sum_grade_status_len",
        "s.score_share_of_student_id_term",
        "s.second_enrolled_at",
        F.col("c.level").alias("course_level"),
    )
)

result = (
    enriched
    .filter(F.col("course_level") != "intro")
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "grade_status_len_share_of_student_id_term",
        "student_id_term_sum_grade_status_len",
        "score_share_of_student_id_term",
        "course_level",
    )
    .orderBy(F.col("score_share_of_student_id_term").desc())
)
