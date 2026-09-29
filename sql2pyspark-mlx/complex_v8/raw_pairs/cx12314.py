from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_latest = Window.partitionBy("grade_status", "term").orderBy(F.col("avg_score").desc(), F.col("id").desc())

filtered = (
    df_enrollments
    .filter(F.col("student_id").isNotNull())
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
    )
)

term_stats = (
    filtered
    .groupBy("term")
    .agg(F.avg("score").alias("avg_score"))
)

above_average = (
    filtered.alias("s")
    .join(term_stats.alias("g"), F.col("g.term") == F.col("s.term"), "inner")
    .filter(F.col("s.score") > F.col("g.avg_score"))
    .select(
        "s.*",
        "g.avg_score",
        F.round(F.col("s.score") - F.col("g.avg_score"), 2).alias("score_minus_avg"),
    )
)

deduped = (
    above_average
    .withColumn("recency_rank", F.row_number().over(w_latest))
    .filter(F.col("recency_rank") == 1)
    .drop("recency_rank")
)

annotated = (
    deduped
    .filter(F.col("course_id") <= 4)
    .select(
        "*",
        F.greatest(F.col("avg_score"), F.col("score_minus_avg")).alias("greatest_avg_score_score_minus_avg"),
        F.concat(F.col("grade_status"), F.lit("#"), F.col("id").cast("string")).alias("grade_status_ref"),
    )
)

with_details_course = (
    annotated.alias("s")
    .join(df_courses.alias("c"), (F.col("c.id") == F.col("s.course_id")) & (F.col("c.dept").isNotNull()), "inner")
    .join(df_students.alias("st"), F.col("st.id") == F.col("s.student_id"), "left")
    .filter(F.col("c.level") == "advanced")
    .select(
        "s.id",
        "s.student_id",
        "s.course_id",
        "s.term",
        "s.score",
        "s.grade_status",
        "s.avg_score",
        "s.greatest_avg_score_score_minus_avg",
        "s.grade_status_ref",
        F.col("c.level").alias("course_level"),
        F.col("st.major").alias("student_major"),
        F.col("st.cohort").alias("student_cohort"),
    )
)

detail_rows_enrollments = (
    with_details_course.alias("s")
    .join(df_enrollments.alias("e"), F.col("e.student_id") == F.col("s.student_id"), "inner")
    .select(
        "s.id",
        "s.student_id",
        "s.course_id",
        "s.score",
        "s.grade_status",
        "s.greatest_avg_score_score_minus_avg",
        "s.course_level",
        "s.student_cohort",
        F.col("e.id").alias("enrollment_id"),
        F.col("e.enrolled_at").alias("enrollment_enrolled_at"),
        F.col("e.grade_status").alias("enrollment_grade_status"),
        F.col("e.term").alias("enrollment_term"),
        F.col("e.score").alias("enrollment_score"),
    )
)

result = (
    detail_rows_enrollments
    .groupBy("enrollment_term", "course_level")
    .pivot("enrollment_grade_status", ["FAIL", "WITHDRAWN", "PASS"])
    .agg(F.sum("score"))
    .orderBy("enrollment_term", "course_level")
)
