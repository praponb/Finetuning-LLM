from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_score_full = Window.orderBy(F.col("score").desc_nulls_first(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_score = Window.orderBy(F.col("score").desc_nulls_first(), "id")

filtered = (
    df_enrollments
    .filter(F.col("course_id").isNotNull())
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
    )
)

sequenced = (
    filtered
    .select(
        "id",
        "student_id",
        "course_id",
        "score",
        "grade_status",
        F.nth_value("score", 2).over(w_all_by_score_full).alias("second_score"),
        F.lead("score", 2).over(w_all_by_score).alias("next2_score"),
    )
)

detail_rows = (
    sequenced.alias("s")
    .join(df_enrollments.alias("e"), F.col("e.student_id") == F.col("s.student_id"), "left")
    .select(
        "s.id",
        "s.student_id",
        "s.course_id",
        "s.score",
        "s.grade_status",
        "s.second_score",
        "s.next2_score",
        F.col("e.id").alias("enrollment_id"),
        F.col("e.score").alias("enrollment_score"),
        F.col("e.grade_status").alias("enrollment_grade_status"),
    )
)

scored = (
    detail_rows
    .filter(F.col("enrollment_score") <= 71.0)
    .select(
        "id",
        "student_id",
        "course_id",
        "second_score",
        "next2_score",
        "enrollment_id",
        "enrollment_score",
        "enrollment_grade_status",
        F.lower(F.regexp_replace(F.trim(F.col("grade_status")), " ", "_")).alias("grade_status_slug"),
    )
)

screened = (
    scored.alias("s")
    .join(df_students.alias("st"), (F.col("st.id") == F.col("s.student_id")) & (~F.col("st.is_international")), "left_anti")
)

result = (
    screened
    .select(
        "id",
        "student_id",
        "course_id",
        "next2_score",
        "enrollment_id",
        "grade_status_slug",
    )
)
