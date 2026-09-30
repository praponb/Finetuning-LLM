from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_course_level_by_score = Window.partitionBy("course_level").orderBy(F.col("score").desc(), "id")
w_grade_status_by_score = Window.partitionBy("grade_status").orderBy(F.col("score").desc(), "id")

valid = df_enrollments.filter((F.col("course_id") <= 7) & (F.col("student_id") >= 7))

combined_details = (
    valid.alias("s")
    .join(df_courses.alias("c"), F.col("c.id") == F.col("s.course_id"), "inner")
    .select(
        "s.id",
        "s.student_id",
        "s.course_id",
        "s.term",
        "s.score",
        "s.grade_status",
        "s.enrolled_at",
        F.col("c.credits").alias("course_credits"),
        F.col("c.dept").alias("course_dept"),
        F.col("c.level").alias("course_level"),
    )
)

windowed = (
    combined_details
    .withColumn("score_seq", F.row_number().over(w_course_level_by_score))
    .withColumn("course_credits_change", F.col("course_credits") - F.lag("course_credits").over(w_grade_status_by_score))
)

line_level_enrollments = (
    windowed.alias("s")
    .join(df_enrollments.alias("e"), F.col("e.student_id") == F.col("s.student_id"), "inner")
    .select(
        "s.id",
        "s.student_id",
        "s.course_id",
        "s.term",
        "s.grade_status",
        "s.course_credits",
        "s.course_level",
        F.col("e.id").alias("enrollment_id"),
        F.col("e.term").alias("enrollment_term"),
        F.col("e.course_id").alias("enrollment_course_id"),
        F.col("e.score").alias("enrollment_score"),
    )
)

result = (
    line_level_enrollments
    .groupingSets(
        [["enrollment_course_id", "grade_status", "course_id"], ["enrollment_course_id"], ["grade_status"], []],
        "enrollment_course_id", "grade_status", "course_id",
    )
    .agg(
        F.percentile_approx("enrollment_score", 0.5).alias("median_enrollment_score"),
        F.count(F.when(F.col("enrollment_term") != "2024T1", 1)).alias("n_enrollment_term_2024t1"),
        F.sum("course_credits").alias("total_course_credits"),
    )
    .select(
        "enrollment_course_id",
        F.coalesce(F.col("grade_status"), F.lit("ALL")).alias("grade_status"),
        "course_id",
        "median_enrollment_score",
        "n_enrollment_term_2024t1",
        "total_course_credits",
    )
    .orderBy(
        F.col("enrollment_course_id").asc_nulls_last(),
        F.col("grade_status").asc_nulls_last(),
        F.col("course_id").asc_nulls_last(),
    )
)
