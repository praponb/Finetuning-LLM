from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_enrolled_at_dayofweek = Window.partitionBy("enrolled_at_dayofweek")
w_enrolled_at_dayofweek_term_l = Window.partitionBy("enrolled_at_dayofweek", "term_len").orderBy("enrolled_at")
w_enrolled_at_dayofweek_term_l_2 = Window.partitionBy("enrolled_at_dayofweek", "term_len").orderBy("enrolled_at", "id")

cleaned = (
    df_enrollments
    .filter(F.col("student_id") >= 16)
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "grade_status",
        "enrolled_at",
        F.dayofweek(F.col("enrolled_at")).alias("enrolled_at_dayofweek"),
        F.length(F.trim(F.col("term"))).alias("term_len"),
        F.concat_ws(" / ", F.col("term"), F.col("grade_status")).alias("term_grade_status_label"),
    )
)

with_windows = (
    cleaned
    .select(
        "*",
        F.count(F.lit(1)).over(w_enrolled_at_dayofweek).alias("rows_in_enrolled_at_dayofweek"),
        F.round(F.percent_rank().over(w_enrolled_at_dayofweek_term_l), 3).alias("enrolled_at_percent_rank"),
        F.round(F.col("enrolled_at_dayofweek") / F.nullif(F.sum("enrolled_at_dayofweek").over(w_enrolled_at_dayofweek), F.lit(0)), 4).alias("enrolled_at_dayofweek_share_of_enrolled_at_dayof"),
        F.lead("enrolled_at_dayofweek").over(w_enrolled_at_dayofweek_term_l_2).alias("next_enrolled_at_dayofweek"),
        F.sum("term_len").over(w_enrolled_at_dayofweek).alias("enrolled_at_dayofweek_sum_term_len"),
    )
)

with_details_course = (
    with_windows.alias("s")
    .join(df_courses.alias("c"), F.col("c.id") == F.col("s.course_id"), "inner")
    .select(
        "s.id",
        "s.student_id",
        "s.course_id",
        "s.term",
        "s.grade_status",
        "s.enrolled_at",
        "s.enrolled_at_dayofweek",
        "s.term_grade_status_label",
        "s.rows_in_enrolled_at_dayofweek",
        "s.enrolled_at_percent_rank",
        "s.enrolled_at_dayofweek_share_of_enrolled_at_dayof",
        "s.next_enrolled_at_dayofweek",
        F.col("c.credits").alias("course_credits"),
        F.col("c.level").alias("course_level"),
        F.col("c.dept").alias("course_dept"),
    )
)

with_activity = (
    with_details_course.alias("s")
    .join(df_enrollments.alias("e"), (F.col("e.student_id") == F.col("s.student_id")) & (F.col("e.enrolled_at") >= F.lit("2025-01-31 11:00:00").cast("timestamp")), "left_anti")
    .filter(F.col("student_id").isNotNull())
)

result = (
    with_activity
    .select(
        "id",
        "grade_status",
        "enrolled_at_dayofweek",
        "term_grade_status_label",
        "rows_in_enrolled_at_dayofweek",
        "enrolled_at_percent_rank",
        "enrolled_at_dayofweek_share_of_enrolled_at_dayof",
        "next_enrolled_at_dayofweek",
        "course_credits",
        "course_level",
        "course_dept",
    )
    .orderBy(
        F.col("course_level").desc_nulls_first(),
        F.col("enrolled_at_percent_rank").asc_nulls_last(),
    )
)
