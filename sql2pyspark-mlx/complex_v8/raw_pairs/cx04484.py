from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_max_enrollment_score = Window.orderBy(F.col("max_enrollment_score").desc())
w_course_credits_by_enrolled_a = Window.partitionBy("course_credits").orderBy(F.col("enrolled_at").desc(), "id")
w_student_cohort = Window.partitionBy("student_cohort")

valid_enrollments = (
    df_enrollments
    .filter(F.col("student_id").isNotNull())
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

with_parent = (
    valid_enrollments.alias("s")
    .join(df_students.alias("st"), F.col("st.id") == F.col("s.student_id"), "inner")
    .join(df_courses.alias("c"), F.col("c.id") == F.col("s.course_id"), "left")
    .select(
        "s.id",
        "s.student_id",
        "s.course_id",
        "s.grade_status",
        "s.enrolled_at",
        F.col("st.cohort").alias("student_cohort"),
        F.col("st.enrolled_on").alias("student_enrolled_on"),
        F.col("c.level").alias("course_level"),
        F.col("c.credits").alias("course_credits"),
    )
)

course_level_stats = (
    with_parent
    .groupBy("course_level")
    .agg(F.avg("course_credits").alias("avg_course_credits"))
)

compared = (
    with_parent.alias("s")
    .join(course_level_stats.alias("g"), F.col("g.course_level") == F.col("s.course_level"), "inner")
    .filter(F.col("s.course_credits") > F.col("g.avg_course_credits"))
    .select(
        "s.*",
        "g.avg_course_credits",
        F.round(F.col("s.course_credits") - F.col("g.avg_course_credits"), 2).alias("course_credits_minus_avg"),
    )
)

enriched = (
    compared.alias("s")
    .join(df_students.alias("st"), F.col("st.id") == F.col("s.student_id"), "left")
    .select(
        "s.id",
        "s.student_id",
        "s.course_id",
        "s.grade_status",
        "s.enrolled_at",
        "s.student_cohort",
        "s.student_enrolled_on",
        "s.course_level",
        "s.course_credits",
        "s.course_credits_minus_avg",
        F.col("st.is_international").alias("student_is_international"),
    )
)

enrollment_stats = (
    df_enrollments
    .groupBy("course_id")
    .agg(
        F.min("enrolled_at").alias("first_enrollment_enrolled_at"),
        F.max("enrolled_at").alias("last_enrollment_enrolled_at"),
        F.max("score").alias("max_enrollment_score"),
    )
)

with_counts = (
    enriched.alias("s")
    .join(enrollment_stats.alias("e"), F.col("e.course_id") == F.col("s.course_id"), "left")
    .select(
        "s.*",
        "e.first_enrollment_enrolled_at",
        "e.last_enrollment_enrolled_at",
        "e.max_enrollment_score",
    )
)

scored = (
    with_counts
    .select(
        "*",
        F.round(F.col("course_credits_minus_avg") / F.nullif(F.col("max_enrollment_score"), F.lit(0)), 3).alias("course_credits_minus_avg_per_max_enrollment_scor"),
        (F.col("course_credits") - F.col("course_credits_minus_avg")).alias("course_credits_minus_course_credits_minus_avg"),
    )
)

with_windows = (
    scored
    .select(
        "id",
        "course_id",
        "enrolled_at",
        "student_cohort",
        "student_enrolled_on",
        "course_level",
        "course_credits",
        "course_credits_minus_avg",
        "first_enrollment_enrolled_at",
        "last_enrollment_enrolled_at",
        "course_credits_minus_avg_per_max_enrollment_scor",
        "course_credits_minus_course_credits_minus_avg",
        F.round(F.cume_dist().over(w_all_by_max_enrollment_score), 3).alias("max_enrollment_score_cume_dist"),
        F.round(F.percent_rank().over(w_course_credits_by_enrolled_a), 3).alias("enrolled_at_percent_rank"),
        F.count(F.lit(1)).over(w_student_cohort).alias("rows_in_student_cohort"),
    )
)

result = (
    with_windows
    .groupBy("course_id", "course_level")
    .pivot("student_cohort", ["C2022", "C2024", "C2021"])
    .agg(F.min("course_credits_minus_avg"))
    .orderBy("course_id", "course_level")
)
