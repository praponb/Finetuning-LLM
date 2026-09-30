from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_term_last_enrollment_enrolle = Window.partitionBy("term", F.year("last_enrollment_enrolled_at"))
w_enrolled_at_to_date_grade_st = Window.partitionBy(F.to_date("enrolled_at"), "grade_status").orderBy(F.col("score_bucket").desc(), "id")
w_course_id = Window.partitionBy("course_id")
w_all_by_score_bucket = Window.orderBy("score_bucket", "id")

prepared_enrollments = (
    df_enrollments
    .filter(~F.col("term").isin("2025T2", "2024T1"))
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "grade_status",
        "enrolled_at",
        (F.floor(F.col("score") / 25) * 25).alias("score_bucket"),
    )
)

without_matches = (
    prepared_enrollments.alias("s")
    .join(df_enrollments.alias("e"), (F.col("e.course_id") == F.col("s.course_id")) & (F.col("e.term") != "2025T2") & (F.col("e.grade_status") == "INCOMPLETE"), "left_semi")
)

enrollment_stats = (
    df_enrollments
    .groupBy("course_id")
    .agg(F.max("enrolled_at").alias("last_enrollment_enrolled_at"))
)

with_stats = (
    without_matches.alias("s")
    .join(enrollment_stats.alias("e"), F.col("e.course_id") == F.col("s.course_id"), "left")
    .select("s.*", "e.last_enrollment_enrolled_at")
)

with_trends = (
    with_stats
    .select(
        "id",
        "course_id",
        "term",
        "grade_status",
        "score_bucket",
        F.round(F.col("score_bucket") / F.nullif(F.sum("score_bucket").over(w_term_last_enrollment_enrolle), F.lit(0)), 4).alias("score_bucket_share_of_term_last_enrollment_enrol"),
        F.size(F.collect_set("student_id").over(w_term_last_enrollment_enrolle)).alias("distinct_student_id_in_term_last_enrollment_enro"),
        F.round(F.cume_dist().over(w_enrolled_at_to_date_grade_st), 3).alias("score_bucket_cume_dist"),
    )
)

with_details_course = (
    with_trends.alias("s")
    .join(df_courses.alias("c"), F.col("c.id") == F.col("s.course_id"), "inner")
    .select("s.*", "c.course_code", F.col("c.credits").alias("course_credits"))
)

with_parent_course = (
    with_details_course.alias("s")
    .join(df_courses.alias("c"), (F.col("c.id") == F.col("s.course_id")) & (F.col("c.dept").isin("Biz", "Math", "Bio")), "inner")
    .filter(F.col("c.level") != "core")
    .select(
        "s.id",
        "s.course_id",
        "s.score_bucket",
        "s.distinct_student_id_in_term_last_enrollment_enro",
        "s.course_code",
        "s.course_credits",
        F.col("c.credits").alias("course_credits_2"),
        F.col("c.level").alias("course_level"),
    )
)

with_windows = (
    with_parent_course
    .select(
        "id",
        "course_id",
        "distinct_student_id_in_term_last_enrollment_enro",
        "course_code",
        "course_credits",
        "course_credits_2",
        F.count(F.lit(1)).over(w_course_id).alias("rows_in_course_id"),
        F.lag("course_credits", 2).over(w_all_by_score_bucket).alias("prev2_course_credits"),
    )
)

result = (
    with_windows
    .filter(F.col("course_credits") <= 3)
    .select(
        "id",
        "course_id",
        "distinct_student_id_in_term_last_enrollment_enro",
        "course_code",
        "course_credits",
        "rows_in_course_id",
        "prev2_course_credits",
        F.length(F.trim(F.col("course_code"))).alias("course_code_len"),
    )
    .orderBy("id", F.col("course_code_len").desc())
)
