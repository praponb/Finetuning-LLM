from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_course_dept_student_id = Window.partitionBy("course_dept", "student_id")
w_course_credits_by_student_gp = Window.partitionBy("course_credits").orderBy(F.col("student_gpa").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_course_credits_by_student_gp_2 = Window.partitionBy("course_credits").orderBy(F.col("student_gpa").desc(), "id")

filtered = (
    df_enrollments
    .filter(F.col("score") >= 16.3)
    .select(
        "id",
        "student_id",
        "course_id",
        "score",
        "grade_status",
        "enrolled_at",
    )
)

filtered_2 = (
    filtered
    .crossJoin(filtered.agg(F.max("score").alias("overall_max_score")))
    .filter(F.col("score") < F.col("overall_max_score"))
    .drop("overall_max_score")
)

with_parent_course = (
    filtered_2.alias("s")
    .join(df_courses.alias("c"), F.col("c.id") == F.col("s.course_id"), "inner")
    .join(df_students.alias("st"), F.col("st.id") == F.col("s.student_id"), "left")
    .select(
        "s.id",
        "s.student_id",
        "s.course_id",
        "s.score",
        "s.grade_status",
        "s.enrolled_at",
        "c.course_code",
        "st.student_name",
        F.col("st.gpa").alias("student_gpa"),
    )
)

with_details_course = (
    with_parent_course.alias("s")
    .join(df_courses.alias("c"), F.col("c.id") == F.col("s.course_id"), "inner")
    .filter(F.col("c.dept") == "Bio")
    .select(
        "s.id",
        "s.student_id",
        "s.course_id",
        "s.score",
        "s.course_code",
        "s.student_name",
        "s.student_gpa",
        F.col("c.course_code").alias("course_course_code"),
        F.col("c.dept").alias("course_dept"),
        F.col("c.credits").alias("course_credits"),
    )
)

derived = (
    with_details_course
    .filter(F.col("student_id").isNotNull())
    .withColumn("student_gpa_distance", F.abs(F.col("student_gpa") - 2.9))
)

sequenced = (
    derived
    .select(
        "id",
        "course_id",
        "score",
        "student_name",
        "course_credits",
        F.count(F.lit(1)).over(w_course_dept_student_id).alias("rows_in_course_dept_student_id"),
        F.size(F.collect_set("course_id").over(w_course_dept_student_id)).alias("distinct_course_id_in_course_dept_student_id"),
        F.min("score").over(w_course_credits_by_student_gp).alias("min_score_so_far"),
        F.lead("course_credits").over(w_course_credits_by_student_gp_2).alias("next_course_credits"),
    )
)

joined = (
    sequenced.alias("s")
    .join(df_courses.alias("c"), F.col("c.id") == F.col("s.course_id"), "inner")
    .select(
        "s.id",
        "s.course_id",
        "s.score",
        "s.student_name",
        "s.course_credits",
        "s.rows_in_course_dept_student_id",
        "s.distinct_course_id_in_course_dept_student_id",
        "s.next_course_credits",
        F.col("c.level").alias("course_level"),
    )
)

result = (
    joined
    .select(
        "id",
        "rows_in_course_dept_student_id",
        "next_course_credits",
        "course_level",
    )
    .orderBy(
        F.col("next_course_credits").asc_nulls_last(),
        F.col("rows_in_course_dept_student_id").desc_nulls_first(),
        "id",
    )
    .limit(20)
)
