from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_term_by_score_full = Window.partitionBy("term").orderBy(F.col("score").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_term_by_score = Window.partitionBy("term").orderBy(F.col("score").desc(), "id")
w_student_id_term_by_score = Window.partitionBy("student_id", "term").orderBy(F.col("score").desc(), "id")
w_course_id = Window.partitionBy("course_id")

scoped = (
    df_enrollments
    .filter(~F.col("grade_status").isin("PASS", "FAIL"))
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
        "enrolled_at",
    )
)

enriched = (
    scoped.alias("s")
    .join(df_students.alias("st"), F.col("st.id") == F.col("s.student_id"), "inner")
    .select(
        "s.id",
        "s.student_id",
        "s.course_id",
        "s.term",
        "s.score",
        "s.enrolled_at",
        F.col("st.enrolled_on").alias("student_enrolled_on"),
        F.col("st.major").alias("student_major"),
        F.col("st.cohort").alias("student_cohort"),
    )
)

without_matches = (
    enriched.alias("s")
    .join(df_enrollments.alias("e"), F.col("e.course_id") == F.col("s.course_id"), "left_semi")
)

with_trends = (
    without_matches
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "enrolled_at",
        "student_enrolled_on",
        "student_cohort",
        F.first("student_enrolled_on").over(w_term_by_score_full).alias("first_student_enrolled_on"),
        F.dense_rank().over(w_term_by_score).alias("score_dense_rank"),
    )
)

sequenced = (
    with_trends
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "enrolled_at",
        "student_enrolled_on",
        "score_dense_rank",
        F.datediff(F.col("student_enrolled_on"), F.lag("student_enrolled_on").over(w_student_id_term_by_score)).alias("days_since_prev_student_enrolled_on"),
        F.rank().over(w_student_id_term_by_score).alias("score_rank"),
        F.round(F.col("score") / F.nullif(F.sum("score").over(w_course_id), F.lit(0)), 4).alias("score_share_of_course_id"),
    )
)

without_matches_2 = (
    sequenced.alias("s")
    .join(df_enrollments.alias("e"), (F.col("e.student_id") == F.col("s.student_id")) & (F.to_date(F.col("e.enrolled_at")) < F.lit("2025-03-08").cast("date")), "left_anti")
)

result = (
    without_matches_2
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "enrolled_at",
        "score_dense_rank",
        "days_since_prev_student_enrolled_on",
        "score_rank",
        "score_share_of_course_id",
        F.datediff(F.lit("2025-06-30").cast("date"), F.col("enrolled_at")).alias("days_since_enrolled_at"),
    )
)
