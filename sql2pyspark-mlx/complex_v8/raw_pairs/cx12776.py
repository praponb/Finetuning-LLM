from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_term_course_credits = Window.partitionBy("term", "course_credits")
w_student_enrolled_on_month = Window.partitionBy(F.month("student_enrolled_on"))
w_student_is_international_by = Window.partitionBy("student_is_international").orderBy(F.col("course_credits").desc())

cleaned_enrollments = (
    df_enrollments
    .filter((F.col("enrolled_at") >= F.lit("2025-01-19 07:00:00").cast("timestamp")) & (F.col("grade_status").isin("WITHDRAWN", "INCOMPLETE", "PASS")))
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "grade_status",
        "enrolled_at",
        F.coalesce(F.col("score"), F.lit(0.0)).alias("score_filled"),
    )
)

with_details_course = (
    cleaned_enrollments.alias("s")
    .join(df_courses.alias("c"), F.col("c.id") == F.col("s.course_id"), "inner")
    .join(df_students.alias("st"), F.col("st.id") == F.col("s.student_id"), "left")
    .filter(F.col("c.dept").isin("Bio", "Art", "Biz"))
    .select(
        "s.id",
        "s.student_id",
        "s.course_id",
        "s.term",
        "s.grade_status",
        "s.enrolled_at",
        "s.score_filled",
        F.col("c.credits").alias("course_credits"),
        F.col("c.dept").alias("course_dept"),
        F.col("st.enrolled_on").alias("student_enrolled_on"),
        F.col("st.is_international").alias("student_is_international"),
    )
)

sequenced = (
    with_details_course
    .select(
        "*",
        F.size(F.collect_set("course_id").over(w_term_course_credits)).alias("distinct_course_id_in_term_course_credits"),
        F.round(F.col("score_filled") / F.nullif(F.sum("score_filled").over(w_term_course_credits), F.lit(0)), 4).alias("score_filled_share_of_term_course_credits"),
        F.count(F.lit(1)).over(w_term_course_credits).alias("rows_in_term_course_credits"),
        F.size(F.collect_set("grade_status").over(w_term_course_credits)).alias("distinct_grade_status_in_term_course_credits"),
        F.round(F.col("score_filled") - F.avg("score_filled").over(w_term_course_credits), 2).alias("score_filled_vs_term_course_credits_avg"),
        F.round(F.col("course_credits") / F.nullif(F.sum("course_credits").over(w_student_enrolled_on_month), F.lit(0)), 4).alias("course_credits_share_of_student_enrolled_on_mont"),
        F.dense_rank().over(w_student_is_international_by).alias("course_credits_dense_rank"),
    )
)

result = (
    sequenced
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "enrolled_at",
        "score_filled",
        "course_credits",
        "course_dept",
        "student_is_international",
        "rows_in_term_course_credits",
        "distinct_grade_status_in_term_course_credits",
        "score_filled_vs_term_course_credits_avg",
        "course_credits_share_of_student_enrolled_on_mont",
        "course_credits_dense_rank",
        F.coalesce(F.col("score_filled"), F.lit(0.0)).alias("score_filled_filled"),
        F.when(F.col("student_is_international"), 1).otherwise(0).alias("student_is_international_flag"),
    )
    .orderBy("course_credits", "term")
)
