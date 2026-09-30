from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_student_is_international_stu = Window.partitionBy("student_is_international", "student_cohort").orderBy(F.col("enrolled_at").desc(), "id")
w_enrolled_at_year = Window.partitionBy(F.year("enrolled_at"))
w_student_id_course_id_by_cour = Window.partitionBy("student_id", "course_id").orderBy(F.col("course_credits").asc_nulls_last())
w_student_cohort_term_by_cours = Window.partitionBy("student_cohort", "term").orderBy("course_credits", "id")
w_student_id_course_id_by_cour_2 = Window.partitionBy("student_id", "course_id").orderBy(F.col("course_credits").asc_nulls_last(), "id")

filtered_enrollments = (
    df_enrollments
    .filter(F.col("term") == "2025T2")
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
        "enrolled_at",
        F.initcap(F.trim(F.col("grade_status"))).alias("grade_status_clean"),
        F.when(F.col("score") >= 62.5, "HIGH").when(F.col("score") >= 17.4, "MEDIUM").otherwise("LOW").alias("score_tier"),
    )
)

joined = (
    filtered_enrollments.alias("s")
    .join(df_students.alias("st"), F.col("st.id") == F.col("s.student_id"), "inner")
    .join(df_courses.alias("c"), F.col("c.id") == F.col("s.course_id"), "inner")
    .filter(~F.col("st.cohort").isin("C2023", "C2024"))
    .select(
        "s.id",
        "s.student_id",
        "s.course_id",
        "s.term",
        "s.enrolled_at",
        "s.score_tier",
        F.col("st.is_international").alias("student_is_international"),
        F.col("st.cohort").alias("student_cohort"),
        F.col("c.credits").alias("course_credits"),
    )
)

with_windows = (
    joined
    .select(
        "*",
        F.round(F.cume_dist().over(w_student_is_international_stu), 3).alias("enrolled_at_cume_dist"),
        F.sum("course_credits").over(w_enrolled_at_year).alias("enrolled_at_year_sum_course_credits"),
        F.max("course_credits").over(w_enrolled_at_year).alias("enrolled_at_year_max_course_credits"),
        F.round(F.cume_dist().over(w_student_id_course_id_by_cour), 3).alias("course_credits_cume_dist"),
        F.lag("enrolled_at").over(w_student_cohort_term_by_cours).alias("prev_enrolled_at"),
        F.dense_rank().over(w_student_id_course_id_by_cour_2).alias("course_credits_dense_rank"),
    )
)

result = (
    with_windows
    .select(
        "id",
        "enrolled_at",
        "score_tier",
        "student_is_international",
        "student_cohort",
        "course_credits",
        "enrolled_at_cume_dist",
        "enrolled_at_year_sum_course_credits",
        "course_credits_cume_dist",
        "prev_enrolled_at",
        "course_credits_dense_rank",
        F.round((F.unix_timestamp(F.col("prev_enrolled_at")) - F.unix_timestamp(F.col("enrolled_at"))) / 3600, 1).alias("hours_enrolled_at_to_prev_enrolled_at"),
        F.lower(F.trim(F.col("score_tier"))).alias("score_tier_clean"),
    )
    .orderBy(F.col("enrolled_at").desc(), F.col("course_credits").desc(), "id")
    .limit(10)
)
