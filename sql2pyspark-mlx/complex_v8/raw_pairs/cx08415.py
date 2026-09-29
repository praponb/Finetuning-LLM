from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_dedup = Window.partitionBy("student_cohort", "score_tier").orderBy(F.col("avg_score").desc(), F.col("id").desc())

base = (
    df_enrollments
    .filter(F.col("course_id") >= 3)
    .select(
        "id",
        "student_id",
        "course_id",
        "score",
        "grade_status",
        "enrolled_at",
        F.when(F.col("score") >= 73.0, "large").when(F.col("score") >= 38.5, "medium").otherwise("small").alias("score_tier"),
    )
)

student_id_stats = (
    base
    .groupBy("student_id")
    .agg(F.avg("score").alias("avg_score"))
)

outliers = (
    base.alias("s")
    .join(student_id_stats.alias("g"), F.col("g.student_id") == F.col("s.student_id"), "inner")
    .filter(F.col("s.score") > F.col("g.avg_score"))
    .select(
        "s.*",
        "g.avg_score",
        F.round(F.col("s.score") - F.col("g.avg_score"), 2).alias("score_minus_avg"),
    )
)

with_details_student = (
    outliers.alias("s")
    .join(df_students.alias("st"), F.col("st.id") == F.col("s.student_id"), "inner")
    .join(df_courses.alias("c"), F.col("c.id") == F.col("s.course_id"), "inner")
    .select(
        "s.id",
        "s.student_id",
        "s.course_id",
        "s.score",
        "s.grade_status",
        "s.enrolled_at",
        "s.score_tier",
        "s.avg_score",
        "s.score_minus_avg",
        F.col("st.gpa").alias("student_gpa"),
        F.col("st.cohort").alias("student_cohort"),
        F.col("st.enrolled_on").alias("student_enrolled_on"),
        F.col("c.dept").alias("course_dept"),
        F.col("c.credits").alias("course_credits"),
    )
)

latest_per_key = (
    with_details_student
    .withColumn("recency_rank", F.row_number().over(w_dedup))
    .filter(F.col("recency_rank") == 1)
    .drop("recency_rank")
)

detail_rows_enrollments = (
    latest_per_key.alias("s")
    .join(df_enrollments.alias("e"), F.col("e.student_id") == F.col("s.student_id"), "inner")
    .select(
        "s.id",
        "s.student_id",
        "s.course_id",
        "s.grade_status",
        "s.enrolled_at",
        "s.avg_score",
        "s.student_gpa",
        "s.student_enrolled_on",
        F.col("e.id").alias("enrollment_id"),
        F.col("e.enrolled_at").alias("enrollment_enrolled_at"),
        F.col("e.term").alias("enrollment_term"),
        F.col("e.score").alias("enrollment_score"),
    )
)

totals = (
    detail_rows_enrollments
    .groupBy(
        "enrollment_id",
        F.to_date("enrollment_enrolled_at").alias("enrollment_enrolled_at_day"),
    )
    .agg(
        F.countDistinct("course_id").alias("distinct_course_ids"),
        F.countDistinct("student_id").alias("distinct_student_ids"),
        F.count("*").alias("n_rows"),
    )
)

result = (
    totals
    .filter(F.col("enrollment_enrolled_at_day").isNotNull())
    .select(
        "enrollment_id",
        "enrollment_enrolled_at_day",
        "distinct_course_ids",
        "distinct_student_ids",
        F.when(F.col("distinct_student_ids") >= 7, "HIGH").when(F.col("distinct_student_ids") >= 3, "MEDIUM").otherwise("LOW").alias("distinct_student_ids_tier"),
        F.coalesce(F.col("distinct_student_ids"), F.lit(0)).alias("distinct_student_ids_filled"),
    )
    .orderBy(
        F.col("enrollment_enrolled_at_day").desc(),
        F.col("distinct_student_ids_filled").asc_nulls_last(),
    )
)
