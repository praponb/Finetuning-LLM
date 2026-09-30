from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_grade_status_term_or_unknown = Window.partitionBy("grade_status", "term_or_unknown")
w_grade_status_by_score_filled = Window.partitionBy("grade_status").orderBy("score_filled", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_term_or_unknown_by_score_fil = Window.partitionBy("term_or_unknown").orderBy(F.col("score_filled").desc())

cleaned = (
    df_enrollments
    .filter((F.col("course_id") >= 11) | (F.col("student_id") >= 18))
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
        "enrolled_at",
        F.coalesce(F.col("score"), F.lit(0.0)).alias("score_filled"),
        F.lower(F.regexp_replace(F.trim(F.col("grade_status")), " ", "_")).alias("grade_status_slug"),
        F.coalesce(F.col("term"), F.lit("UNKNOWN")).alias("term_or_unknown"),
    )
)

combined_details_student = (
    cleaned.alias("s")
    .join(df_students.alias("st"), F.col("st.id") == F.col("s.student_id"), "inner")
    .select(
        "s.*",
        F.col("st.gpa").alias("student_gpa"),
        F.col("st.cohort").alias("student_cohort"),
        F.col("st.enrolled_on").alias("student_enrolled_on"),
    )
)

ranked = (
    combined_details_student
    .select(
        "id",
        "student_id",
        "score",
        "grade_status",
        "enrolled_at",
        "score_filled",
        "term_or_unknown",
        "student_cohort",
        "student_enrolled_on",
        F.sum("score_filled").over(w_grade_status_term_or_unknown).alias("grade_status_term_or_unknown_sum_score_filled"),
        F.first("score_filled", ignorenulls=True).over(w_grade_status_by_score_filled).alias("first_known_score_filled"),
        F.round(F.percent_rank().over(w_term_or_unknown_by_score_fil), 3).alias("score_filled_percent_rank"),
    )
)

tagged = (
    ranked
    .filter(F.col("student_id") <= 9)
    .select(
        "id",
        "enrolled_at",
        "score_filled",
        "term_or_unknown",
        "student_cohort",
        "student_enrolled_on",
        "grade_status_term_or_unknown_sum_score_filled",
        "first_known_score_filled",
        F.quarter(F.col("enrolled_at")).alias("enrolled_at_quarter"),
        F.round(F.col("first_known_score_filled") * 1.07, 2).alias("first_known_score_filled_adj"),
        F.coalesce(F.col("term_or_unknown"), F.lit("UNKNOWN")).alias("term_or_unknown_or_unknown"),
    )
)

result = (
    tagged
    .select(
        "id",
        "score_filled",
        "term_or_unknown",
        "student_cohort",
        "student_enrolled_on",
        "grade_status_term_or_unknown_sum_score_filled",
        "first_known_score_filled",
        "enrolled_at_quarter",
        "first_known_score_filled_adj",
        "term_or_unknown_or_unknown",
        F.substring(F.trim(F.col("term_or_unknown")), 1, 2).alias("term_or_unknown_prefix"),
        F.when(F.col("first_known_score_filled") >= 80.0, "A").when(F.col("first_known_score_filled") >= 30.0, "B").otherwise("C").alias("first_known_score_filled_tier"),
    )
    .orderBy(F.col("first_known_score_filled_tier").desc())
)
