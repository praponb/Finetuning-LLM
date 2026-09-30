from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_course_credits_by_course_cre = Window.partitionBy("course_credits").orderBy(F.col("course_credits").desc(), "id")
w_course_dept_student_id = Window.partitionBy("course_dept", "student_id")
w_dedup = Window.partitionBy("course_dept_or_unknown").orderBy("course_credits", "id")

filtered = (
    df_enrollments
    .filter((F.col("grade_status").isNotNull()) & (~F.col("term").isin("2024T1", "2025T2")))
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "enrolled_at",
        F.lower(F.trim(F.col("grade_status"))).alias("grade_status_clean"),
    )
)

with_parent = (
    filtered.alias("s")
    .join(df_courses.alias("c"), F.col("c.id") == F.col("s.course_id"), "inner")
    .filter(F.col("c.dept") == "Biz")
    .select(
        "s.id",
        "s.student_id",
        "s.course_id",
        "s.score",
        "s.enrolled_at",
        "s.grade_status_clean",
        F.col("c.dept").alias("course_dept"),
    )
)

combined_details_course = (
    with_parent.alias("s")
    .join(df_courses.alias("c"), F.col("c.id") == F.col("s.course_id"), "inner")
    .filter(F.col("c.dept") != "Math")
    .select(
        "s.id",
        "s.student_id",
        "s.course_id",
        "s.enrolled_at",
        "s.grade_status_clean",
        "s.course_dept",
        F.col("c.dept").alias("course_dept_2"),
        "c.course_code",
        F.col("c.credits").alias("course_credits"),
    )
)

tagged = (
    combined_details_course
    .withColumn("course_dept_or_unknown", F.coalesce(F.col("course_dept"), F.lit("UNKNOWN")))
)

ranked = (
    tagged
    .select(
        "id",
        "student_id",
        "course_id",
        "enrolled_at",
        "grade_status_clean",
        "course_dept",
        "course_dept_2",
        "course_code",
        "course_credits",
        "course_dept_or_unknown",
        (F.col("course_credits") - F.lag("course_credits").over(w_course_credits_by_course_cre)).alias("course_credits_change"),
        F.count(F.lit(1)).over(w_course_dept_student_id).alias("rows_in_course_dept_student_id"),
    )
)

latest_per_key = (
    ranked
    .withColumn("recency_rank", F.row_number().over(w_dedup))
    .filter(F.col("recency_rank") == 1)
    .drop("recency_rank")
)

result = (
    latest_per_key
    .filter(F.col("course_dept_or_unknown") == "UNKNOWN")
    .select(
        "id",
        "student_id",
        "course_id",
        "grade_status_clean",
        "course_dept",
        "course_credits",
        "course_dept_or_unknown",
        "course_credits_change",
        "rows_in_course_dept_student_id",
        F.lower(F.regexp_replace(F.trim(F.col("course_dept_or_unknown")), " ", "_")).alias("course_dept_or_unknown_slug"),
    )
    .orderBy(F.col("course_dept").desc(), F.col("course_credits").asc(), "id")
    .limit(5)
)
