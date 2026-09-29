from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_dedup = Window.partitionBy("grade_status", "course_id").orderBy("score", "id")

filtered_enrollments = (
    df_enrollments
    .filter(F.col("score") < 73.0)
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
    )
)

derived = (
    filtered_enrollments
    .filter(F.col("score") > 8.7)
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
        F.when(F.col("score") >= 57.8, "HIGH").when(F.col("score") >= 20.6, "MEDIUM").otherwise("LOW").alias("score_tier"),
    )
)

latest = (
    derived
    .withColumn("recency_rank", F.row_number().over(w_dedup))
    .filter(F.col("recency_rank") == 1)
    .drop("recency_rank")
)

combined_details = (
    latest.alias("s")
    .join(df_courses.alias("c"), F.col("c.id") == F.col("s.course_id"), "left")
    .select(
        "s.id",
        "s.student_id",
        "s.course_id",
        "s.term",
        "s.score",
        "s.score_tier",
        F.col("c.credits").alias("course_credits"),
        "c.course_code",
        F.col("c.dept").alias("course_dept"),
    )
)

stats = (
    combined_details
    .filter(F.col("score_tier") != "HIGH")
    .groupBy("term", "student_id", "course_credits")
    .agg(
        F.min("course_credits").alias("min_course_credits"),
        F.round(100 * F.count(F.when(F.col("course_dept") == "Bio", 1)) / F.count("*"), 1).alias("pct_course_dept_bio"),
        F.max("score").alias("max_score_check"),
    )
    .filter(F.col("max_score_check") > 16.3)
    .drop("max_score_check")
)

with_parent_student = (
    stats.alias("s")
    .join(df_students.alias("st"), F.col("st.id") == F.col("s.student_id"), "inner")
    .select(
        "s.term",
        "s.student_id",
        "s.course_credits",
        "s.min_course_credits",
        "s.pct_course_dept_bio",
        F.col("st.enrolled_on").alias("student_enrolled_on"),
        F.col("st.cohort").alias("student_cohort"),
    )
)

student_cohort_stats = (
    with_parent_student
    .groupBy("student_cohort")
    .agg(F.percentile_approx("min_course_credits", 0.5).alias("median_min_course_credits"))
)

compared = (
    with_parent_student.alias("s")
    .join(student_cohort_stats.alias("g"), F.col("g.student_cohort") == F.col("s.student_cohort"), "inner")
    .filter(F.col("s.min_course_credits") >= F.col("g.median_min_course_credits"))
    .select(
        "s.*",
        "g.median_min_course_credits",
        F.round(F.col("s.min_course_credits") - F.col("g.median_min_course_credits"), 2).alias("min_course_credits_minus_median"),
    )
)

result = (
    compared
    .select(
        "term",
        "student_id",
        "course_credits",
        "min_course_credits",
    )
    .unpivot(
        ids=["term", "student_id"],
        values=["course_credits", "min_course_credits"],
        variableColumnName="metric_name",
        valueColumnName="reading",
    )
)
