from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_enrolled_at = Window.orderBy(F.col("enrolled_at").desc_nulls_first(), "id")
w_latest = Window.partitionBy("course_id").orderBy("enrolled_at", "id")

valid_enrollments = (
    df_enrollments
    .filter((F.col("grade_status").isin("FAIL", "PASS")) & (F.col("score") < 58.8))
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

combined_details_course = (
    valid_enrollments.alias("s")
    .join(df_courses.alias("c"), F.col("c.id") == F.col("s.course_id"), "left")
    .select(
        "s.id",
        "s.student_id",
        "s.course_id",
        "s.term",
        "s.grade_status",
        "s.enrolled_at",
        F.col("c.dept").alias("course_dept"),
    )
)

sequenced = (
    combined_details_course
    .select(
        "*",
        F.row_number().over(w_all_by_enrolled_at).alias("enrolled_at_seq"),
        F.lag("enrolled_at").over(w_all_by_enrolled_at).alias("prev_enrolled_at"),
        F.round((F.unix_timestamp("enrolled_at") - F.unix_timestamp(F.lag("enrolled_at").over(w_all_by_enrolled_at))) / 60, 1).alias("minutes_since_prev_enrolled_at"),
    )
)

scored = (
    sequenced
    .filter(F.col("term").isin("2025T1", "2024T1"))
    .withColumn("enrolled_at_seq_distance", F.abs(F.col("enrolled_at_seq") - 6))
)

latest_per_key = (
    scored
    .withColumn("row_num", F.row_number().over(w_latest))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

with_children_enrollments = (
    latest_per_key.alias("s")
    .join(df_enrollments.alias("e"), F.col("e.student_id") == F.col("s.student_id"), "inner")
    .select(
        "s.id",
        "s.student_id",
        "s.course_id",
        "s.grade_status",
        "s.course_dept",
        "s.prev_enrolled_at",
        "s.minutes_since_prev_enrolled_at",
        F.col("e.id").alias("enrollment_id"),
        F.col("e.term").alias("enrollment_term"),
        F.col("e.course_id").alias("enrollment_course_id"),
        F.col("e.enrolled_at").alias("enrollment_enrolled_at"),
    )
)

annotated = (
    with_children_enrollments
    .filter(F.col("course_dept") == "Bio")
    .select(
        "*",
        F.when(F.col("grade_status").isin("PASS", "INCOMPLETE"), "group_a").when(F.col("grade_status") == "WITHDRAWN", "group_b").otherwise("other").alias("grade_status_group"),
        F.lower(F.regexp_replace(F.trim(F.col("grade_status")), " ", "_")).alias("grade_status_slug"),
        F.datediff(F.lit("2026-01-01").cast("date"), F.col("prev_enrolled_at")).alias("days_since_prev_enrolled_at"),
    )
)

result = (
    annotated
    .filter(F.hour(F.col("prev_enrolled_at")).between(7, 10))
    .select(
        "id",
        "student_id",
        "grade_status",
        "course_dept",
        "prev_enrolled_at",
        "minutes_since_prev_enrolled_at",
        "enrollment_id",
        "grade_status_group",
        "grade_status_slug",
        F.substring(F.trim(F.col("grade_status_group")), 1, 2).alias("grade_status_group_prefix"),
        F.round((F.unix_timestamp(F.col("prev_enrolled_at")) - F.unix_timestamp(F.col("enrollment_enrolled_at"))) / 60, 1).alias("minutes_enrollment_enrolled_at_to_prev_enrolled_"),
    )
    .orderBy("grade_status_slug", F.col("grade_status").asc())
)
