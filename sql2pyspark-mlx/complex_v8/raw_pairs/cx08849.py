from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_student_id_grade_status_by_c = Window.partitionBy("student_id", "grade_status").orderBy(F.col("course_credits").desc(), "id")
w_grade_status_course_dept = Window.partitionBy("grade_status", "course_dept")
w_student_id_grade_status_by_c_2 = Window.partitionBy("student_id", "grade_status").orderBy(F.col("course_credits").desc())
w_course_level_grade_status_by = Window.partitionBy("course_level", "grade_status").orderBy("score", "id")
w_all_by_enrolled_at = Window.orderBy(F.col("enrolled_at").desc(), "id")
w_course_id = Window.partitionBy("course_id")
w_course_level_grade_status_by_2 = Window.partitionBy("course_level", "grade_status").orderBy("score", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

recent = (
    df_enrollments
    .filter((F.col("course_id").isNotNull()) & (F.col("grade_status").isin("FAIL", "WITHDRAWN", "PASS")))
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
        "enrolled_at",
        F.date_format(F.col("enrolled_at"), "yyyy-MM-dd HH:00").alias("enrolled_at_label"),
    )
)

with_details = (
    recent.alias("s")
    .join(df_courses.alias("c"), F.col("c.id") == F.col("s.course_id"), "inner")
    .filter(F.col("c.dept") == "Math")
    .select(
        "s.*",
        F.col("c.credits").alias("course_credits"),
        F.col("c.dept").alias("course_dept"),
        F.col("c.level").alias("course_level"),
    )
)

with_windows = (
    with_details
    .select(
        "*",
        F.round(F.percent_rank().over(w_student_id_grade_status_by_c), 3).alias("course_credits_percent_rank"),
        F.max("course_credits").over(w_grade_status_course_dept).alias("grade_status_course_dept_max_course_credits"),
        F.round(F.percent_rank().over(w_student_id_grade_status_by_c_2), 3).alias("course_credits_percent_rank_2"),
        F.lead("course_credits").over(w_course_level_grade_status_by).alias("next_course_credits"),
        (F.col("score") - F.lag("score").over(w_all_by_enrolled_at)).alias("score_change"),
        F.lead("enrolled_at").over(w_course_level_grade_status_by).alias("next_enrolled_at"),
        F.round(F.avg("course_credits").over(w_course_id), 2).alias("course_id_avg_course_credits"),
        F.lead("enrolled_at", 2).over(w_student_id_grade_status_by_c).alias("next2_enrolled_at"),
        F.ntile(3).over(w_all_by_enrolled_at).alias("enrolled_at_tercile"),
        F.first("enrolled_at", ignorenulls=True).over(w_course_level_grade_status_by_2).alias("first_known_enrolled_at"),
    )
)

labeled = (
    with_windows
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
        "enrolled_at",
        "enrolled_at_label",
        "course_credits",
        "course_dept",
        "course_level",
        "course_credits_percent_rank_2",
        "next_course_credits",
        "score_change",
        "next_enrolled_at",
        "next2_enrolled_at",
        "enrolled_at_tercile",
        "first_known_enrolled_at",
        F.lower(F.regexp_replace(F.trim(F.col("grade_status")), " ", "_")).alias("grade_status_slug"),
        F.datediff(F.lit("2025-06-30").cast("date"), F.col("next2_enrolled_at")).alias("days_since_next2_enrolled_at"),
        F.when(F.col("course_dept").isin("Biz", "Art"), "group_a").when(F.col("course_dept") == "Bio", "group_b").otherwise("other").alias("course_dept_group"),
    )
)

result = (
    labeled
    .select(
        "id",
        "student_id",
        "term",
        "grade_status",
        "enrolled_at",
        "enrolled_at_label",
        "course_credits",
        "course_level",
        "score_change",
        "next_enrolled_at",
        "next2_enrolled_at",
        "enrolled_at_tercile",
        "first_known_enrolled_at",
        "grade_status_slug",
        "course_dept_group",
        F.when(F.col("course_level").isin("advanced", "intro"), "group_a").when(F.col("course_level") == "core", "group_b").otherwise("other").alias("course_level_group"),
    )
    .orderBy(F.col("course_dept_group").asc_nulls_last())
)
