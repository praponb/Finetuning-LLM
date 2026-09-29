from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_grade_status_course_id_by_en = Window.partitionBy("grade_status", "course_id").orderBy("enrolled_at", "id")
w_course_dept_by_enrolled_at = Window.partitionBy("course_dept").orderBy(F.col("enrolled_at").asc_nulls_last(), "id")
w_course_dept_by_enrolled_at_t = Window.partitionBy("course_dept").orderBy(F.col("enrolled_at").asc_nulls_last())
w_student_id_course_dept_by_en = Window.partitionBy("student_id", "course_dept").orderBy("enrolled_at", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_course_dept_by_enrolled_at_r = Window.partitionBy("course_dept").orderBy(F.col("enrolled_at").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_grade_status_course_id_by_en_2 = Window.partitionBy("grade_status", "course_id").orderBy("enrolled_at")

filtered = (
    df_enrollments
    .filter(F.col("student_id") <= 10)
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "grade_status",
        "enrolled_at",
    )
)

combined_details_course = (
    filtered.alias("s")
    .join(df_courses.alias("c"), F.col("c.id") == F.col("s.course_id"), "left")
    .select(
        "s.id",
        "s.student_id",
        "s.course_id",
        "s.term",
        "s.grade_status",
        "s.enrolled_at",
        F.col("c.dept").alias("course_dept"),
        F.col("c.level").alias("course_level"),
    )
)

with_trends = (
    combined_details_course
    .select(
        "*",
        F.round(F.percent_rank().over(w_grade_status_course_id_by_en), 3).alias("enrolled_at_percent_rank"),
        F.round(F.cume_dist().over(w_course_dept_by_enrolled_at), 3).alias("enrolled_at_cume_dist"),
        F.round(F.percent_rank().over(w_course_dept_by_enrolled_at_t), 3).alias("enrolled_at_percent_rank_2"),
        F.ntile(3).over(w_course_dept_by_enrolled_at).alias("enrolled_at_tercile"),
        F.nth_value("enrolled_at", 2).over(w_student_id_course_dept_by_en).alias("second_enrolled_at"),
        F.first("enrolled_at", ignorenulls=True).over(w_course_dept_by_enrolled_at_r).alias("first_known_enrolled_at"),
        F.round(F.cume_dist().over(w_grade_status_course_id_by_en), 3).alias("enrolled_at_cume_dist_2"),
        F.round(F.cume_dist().over(w_grade_status_course_id_by_en_2), 3).alias("enrolled_at_cume_dist_3"),
        F.ntile(10).over(w_course_dept_by_enrolled_at).alias("enrolled_at_decile"),
    )
)

derived = (
    with_trends
    .select(
        "*",
        F.round(F.col("enrolled_at_tercile") / F.nullif(F.col("enrolled_at_cume_dist_2"), F.lit(0)), 3).alias("enrolled_at_tercile_per_enrolled_at_cume_dist_2"),
        F.coalesce(F.col("term"), F.lit("UNKNOWN")).alias("term_or_unknown"),
    )
)

result = (
    derived
    .select(
        "id",
        "student_id",
        "course_id",
        "enrolled_at",
        "course_dept",
        "course_level",
        "enrolled_at_percent_rank",
        "enrolled_at_cume_dist_3",
        "enrolled_at_decile",
        "enrolled_at_tercile_per_enrolled_at_cume_dist_2",
        F.concat_ws(": ", F.col("term"), F.col("course_level")).alias("term_course_level_label"),
        F.lower(F.regexp_replace(F.trim(F.col("course_level")), " ", "_")).alias("course_level_slug"),
    )
    .orderBy(
        "enrolled_at_percent_rank",
        "enrolled_at_tercile_per_enrolled_at_cume_dist_2",
    )
)
