from pyspark.sql import functions as F

cleaned = (
    df_enrollments
    .filter(F.col("score") < 87.8)
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "grade_status",
        "enrolled_at",
        F.date_trunc("quarter", F.col("enrolled_at")).alias("enrolled_at_quarter"),
    )
)

with_details_course = (
    cleaned.alias("s")
    .join(df_courses.alias("c"), F.col("c.id") == F.col("s.course_id"), "left")
    .join(df_students.alias("st"), F.col("st.id") == F.col("s.student_id"), "left")
    .select(
        "s.id",
        "s.student_id",
        "s.course_id",
        "s.term",
        "s.enrolled_at_quarter",
        F.col("c.dept").alias("course_dept"),
        "c.course_code",
        F.col("c.level").alias("course_level"),
        F.col("st.enrolled_on").alias("student_enrolled_on"),
        F.col("st.cohort").alias("student_cohort"),
    )
)

totals = (
    with_details_course
    .filter(F.col("course_id").isNotNull())
    .groupBy("course_dept")
    .agg(
        F.array_sort(F.collect_set("student_cohort")).alias("student_cohort_list"),
        F.sum(F.when(F.col("course_dept") == "CS", 1).otherwise(0)).alias("n_course_dept_cs"),
        F.count("*").alias("row_count"),
    )
)

enriched = (
    totals
    .filter(F.col("n_course_dept_cs").between(3, 7))
    .select(
        "course_dept",
        "student_cohort_list",
        "n_course_dept_cs",
        "row_count",
        F.when(F.col("course_dept").isin("Bio", "Math"), "group_a").when(F.col("course_dept") == "Biz", "group_b").otherwise("other").alias("course_dept_group"),
    )
)

result = (
    enriched
    .select(
        "course_dept",
        "student_cohort_list",
        "n_course_dept_cs",
        "row_count",
        "course_dept_group",
        F.substring(F.trim(F.col("course_dept_group")), 1, 2).alias("course_dept_group_prefix"),
        F.concat_ws(": ", F.col("course_dept"), F.col("course_dept_group")).alias("course_dept_course_dept_group_label"),
    )
    .orderBy("course_dept", F.col("row_count").desc())
)
