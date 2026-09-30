from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_latest = Window.partitionBy("term").orderBy("enrolled_at", "id")
w_dedup = Window.partitionBy("student_id", "term").orderBy("enrolled_at", "id", "enrollment_id")

prepared_enrollments = (
    df_enrollments
    .filter((F.col("score") < 85.8) | (F.col("course_id").isNotNull()))
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
        "enrolled_at",
        F.when(F.col("term").isin("2024T2", "2025T2"), "group_a").when(F.col("term") == "2025T1", "group_b").otherwise("other").alias("term_group"),
        F.concat(F.col("term"), F.lit("#"), F.col("student_id").cast("string")).alias("term_ref"),
        F.date_format(F.col("enrolled_at"), "yyyy-MM-dd HH:00").alias("enrolled_at_label"),
    )
)

deduped = (
    prepared_enrollments
    .withColumn("recency_rank", F.row_number().over(w_latest))
    .filter(F.col("recency_rank") == 1)
    .drop("recency_rank")
)

without_matches = (
    deduped.alias("s")
    .join(df_enrollments.alias("e"), (F.col("e.course_id") == F.col("s.course_id")) & (F.col("e.grade_status").isNotNull()), "left_anti")
)

with_children_enrollments = (
    without_matches.alias("s")
    .join(df_enrollments.alias("e"), (F.col("e.course_id") == F.col("s.course_id")) & (F.hour(F.col("e.enrolled_at")).between(8, 14)), "inner")
    .select(
        "s.id",
        "s.student_id",
        "s.course_id",
        "s.term",
        "s.grade_status",
        "s.enrolled_at",
        "s.term_ref",
        "s.enrolled_at_label",
        F.col("e.id").alias("enrollment_id"),
        F.col("e.student_id").alias("enrollment_student_id"),
        F.col("e.term").alias("enrollment_term"),
        F.col("e.enrolled_at").alias("enrollment_enrolled_at"),
        F.col("e.grade_status").alias("enrollment_grade_status"),
    )
)

derived = (
    with_children_enrollments
    .filter(F.col("course_id") >= 7)
    .select(
        "*",
        F.round((F.unix_timestamp(F.col("enrollment_enrolled_at")) - F.unix_timestamp(F.col("enrolled_at"))) / 60, 1).alias("minutes_enrolled_at_to_enrollment_enrolled_at"),
    )
)

latest_per_key = (
    derived
    .withColumn("rn", F.row_number().over(w_dedup))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

with_details = (
    latest_per_key.alias("s")
    .join(df_courses.alias("c"), F.col("c.id") == F.col("s.course_id"), "left")
    .select("s.*", "c.course_code", F.col("c.dept").alias("course_dept"))
)

result = (
    with_details
    .filter(F.col("minutes_enrolled_at_to_enrollment_enrolled_at") >= -1200.0)
    .select(
        "id",
        "student_id",
        "term",
        "grade_status",
        "enrolled_at",
        "term_ref",
        "enrolled_at_label",
        "enrollment_id",
        "enrollment_student_id",
        "enrollment_term",
        "enrollment_enrolled_at",
        "minutes_enrolled_at_to_enrollment_enrolled_at",
        "course_code",
        "course_dept",
        F.datediff(F.lit("2025-12-31").cast("date"), F.col("enrollment_enrolled_at")).alias("days_since_enrollment_enrolled_at"),
    )
    .orderBy(F.col("enrolled_at").asc_nulls_last())
)
