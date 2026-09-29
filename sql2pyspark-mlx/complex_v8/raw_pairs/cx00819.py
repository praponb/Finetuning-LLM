from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_grade_status_enrolled_at_plu = Window.partitionBy("grade_status", F.to_date("enrolled_at_plus_2h")).orderBy(F.col("enrolled_at_hour").desc(), "id")
w_term = Window.partitionBy("term")
w_grade_status_enrolled_at_plu_2 = Window.partitionBy("grade_status", F.to_date("enrolled_at_plus_2h")).orderBy(F.col("enrolled_at_hour").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

filtered = (
    df_enrollments
    .filter((F.col("grade_status") != "FAIL") & (F.col("course_id").isNotNull()))
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "grade_status",
        "enrolled_at",
        F.hour(F.col("enrolled_at")).alias("enrolled_at_hour"),
        (F.col("enrolled_at") + F.expr("INTERVAL 2 HOURS")).alias("enrolled_at_plus_2h"),
    )
)

without_matches = (
    filtered.alias("s")
    .join(df_enrollments.alias("e"), (F.col("e.course_id") == F.col("s.course_id")) & (F.col("e.student_id") <= 15), "left_semi")
)

metrics = (
    without_matches
    .select(
        "id",
        "student_id",
        "grade_status",
        "enrolled_at",
        "enrolled_at_plus_2h",
        F.lead("enrolled_at").over(w_grade_status_enrolled_at_plu).alias("next_enrolled_at"),
        F.max("enrolled_at_hour").over(w_term).alias("term_max_enrolled_at_hour"),
        F.last("enrolled_at_hour").over(w_grade_status_enrolled_at_plu_2).alias("last_enrolled_at_hour"),
        F.lag("enrolled_at").over(w_grade_status_enrolled_at_plu).alias("prev_enrolled_at"),
    )
)

tagged = (
    metrics
    .filter(F.hour(F.col("prev_enrolled_at")).between(9, 15))
    .select(
        "id",
        "student_id",
        "grade_status",
        "enrolled_at_plus_2h",
        "next_enrolled_at",
        "term_max_enrolled_at_hour",
        "prev_enrolled_at",
        F.lower(F.regexp_replace(F.trim(F.col("grade_status")), " ", "_")).alias("grade_status_slug"),
        F.when(F.col("last_enrolled_at_hour") >= 13, "A").when(F.col("last_enrolled_at_hour") >= 10, "B").otherwise("C").alias("last_enrolled_at_hour_tier"),
    )
)

joined = (
    tagged.alias("s")
    .join(df_students.alias("st"), F.col("st.id") == F.col("s.student_id"), "inner")
    .select(
        "s.id",
        "s.student_id",
        "s.grade_status",
        "s.enrolled_at_plus_2h",
        "s.next_enrolled_at",
        "s.term_max_enrolled_at_hour",
        "s.prev_enrolled_at",
        "s.grade_status_slug",
        "s.last_enrolled_at_hour_tier",
        F.col("st.enrolled_on").alias("student_enrolled_on"),
    )
)

detail_rows = (
    joined.alias("s")
    .join(df_enrollments.alias("e"), (F.col("e.student_id") == F.col("s.student_id")) & (F.col("e.term") == "2024T1"), "left")
    .select(
        "s.id",
        "s.student_id",
        "s.grade_status",
        "s.grade_status_slug",
        "s.last_enrolled_at_hour_tier",
        F.col("e.id").alias("enrollment_id"),
        F.col("e.term").alias("enrollment_term"),
        F.col("e.enrolled_at").alias("enrollment_enrolled_at"),
        F.col("e.course_id").alias("enrollment_course_id"),
    )
)

result = (
    detail_rows
    .select(
        "id",
        "student_id",
        "grade_status",
        "grade_status_slug",
        "enrollment_enrolled_at",
    )
    .orderBy(F.col("enrollment_enrolled_at").desc(), F.col("id").desc())
)
