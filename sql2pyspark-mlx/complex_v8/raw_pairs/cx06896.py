from pyspark.sql import functions as F

recent_courses = (
    df_courses
    .filter((F.upper(F.trim(F.col("course_code"))).like("A%")) & (F.col("credits").between(2, 4)))
    .select(
        "id",
        "course_code",
        "dept",
        "level",
        F.when(F.col("dept").isin("Bio", "Math"), "group_a").when(F.col("dept") == "Biz", "group_b").otherwise("other").alias("dept_group"),
    )
)

course_side = recent_courses.select("id")

enrollment_rows = (
    df_enrollments
    .filter((F.col("course_id") <= 8) & (F.col("grade_status").isin("FAIL", "WITHDRAWN")))
    .select("student_id")
)

result = course_side.intersectAll(enrollment_rows)
