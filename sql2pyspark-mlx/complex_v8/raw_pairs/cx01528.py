from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_score_filled_ties = Window.orderBy(F.col("score_filled").desc())
w_term_by_score = Window.partitionBy("term").orderBy("score", "id")
w_all_by_score_filled = Window.orderBy(F.col("score_filled").desc(), "id")

valid = (
    df_enrollments
    .filter((F.col("score") < 85.8) & (F.col("enrolled_at") < F.lit("2025-02-18 08:00:00").cast("timestamp")))
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
        "enrolled_at",
        F.date_trunc("year", F.col("enrolled_at")).alias("enrolled_at_year"),
        F.coalesce(F.col("score"), F.lit(0.0)).alias("score_filled"),
    )
)

ranked = (
    valid
    .select(
        "*",
        F.dense_rank().over(w_all_by_score_filled_ties).alias("score_filled_dense_rank"),
        F.ntile(3).over(w_term_by_score).alias("score_tercile"),
        F.round(F.percent_rank().over(w_all_by_score_filled), 3).alias("score_filled_percent_rank"),
    )
)

with_parent_student = (
    ranked.alias("s")
    .join(df_students.alias("st"), F.col("st.id") == F.col("s.student_id"), "inner")
    .select(
        "s.id",
        "s.student_id",
        "s.course_id",
        "s.term",
        "s.score",
        "s.grade_status",
        "s.enrolled_at",
        "s.enrolled_at_year",
        "s.score_filled",
        "s.score_tercile",
        "s.score_filled_percent_rank",
        F.col("st.major").alias("student_major"),
        F.col("st.is_international").alias("student_is_international"),
        "st.student_name",
    )
)

enrollment_stats = (
    df_enrollments
    .groupBy("course_id")
    .agg(
        F.max("enrolled_at").alias("last_enrollment_enrolled_at"),
        F.sum("score").alias("total_enrollment_score"),
    )
)

profiled = (
    with_parent_student.alias("s")
    .join(enrollment_stats.alias("e"), F.col("e.course_id") == F.col("s.course_id"), "left")
    .select("s.*", "e.last_enrollment_enrolled_at", "e.total_enrollment_score")
)

result = (
    profiled
    .groupingSets(
        [["student_id", "student_is_international"], ["student_id"]],
        "student_id", "student_is_international",
    )
    .agg(
        F.countDistinct("student_major").alias("distinct_student_majors"),
        F.count("*").alias("cnt"),
        F.countDistinct("course_id").alias("distinct_course_ids"),
    )
    .orderBy(
        F.col("student_id").asc_nulls_last(),
        F.col("student_is_international").asc_nulls_last(),
    )
)
