from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_term_enrolled_at_year_by_enr = Window.partitionBy("term", F.year("enrolled_at")).orderBy("enrolled_at")
w_student_id_grade_status_by_e = Window.partitionBy("student_id", "grade_status").orderBy(F.col("enrolled_at").desc_nulls_first(), "id")
w_course_id_by_enrolled_at_plu = Window.partitionBy("course_id").orderBy(F.col("enrolled_at_plus_7d").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_grade_status_enrolled_at_plu = Window.partitionBy("grade_status", F.to_date("enrolled_at_plus_7d"))
w_term_enrollment_count_by_sec = Window.partitionBy("term", "enrollment_count").orderBy("second_score_filled", "id")
w_course_id_enrollment_grade_s = Window.partitionBy("course_id", "enrollment_grade_status_count").orderBy(F.col("enrollment_count").desc(), "id").rowsBetween(-3, -1)
w_enrollment_grade_status_coun = Window.partitionBy("enrollment_grade_status_count").orderBy(F.col("enrolled_at_plus_7d_date").desc_nulls_first(), "id")

recent_enrollments = (
    df_enrollments
    .filter(F.col("grade_status") != "INCOMPLETE")
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
        "enrolled_at",
        F.coalesce(F.col("score"), F.lit(0.0)).alias("score_filled"),
        (F.col("enrolled_at") + F.expr("INTERVAL 7 DAYS")).alias("enrolled_at_plus_7d"),
    )
)

sequenced = (
    recent_enrollments
    .select(
        "*",
        F.round(F.percent_rank().over(w_term_enrolled_at_year_by_enr), 3).alias("enrolled_at_percent_rank"),
        F.row_number().over(w_student_id_grade_status_by_e).alias("enrolled_at_seq"),
        F.nth_value("score_filled", 2).over(w_course_id_by_enrolled_at_plu).alias("second_score_filled"),
        F.max("score_filled").over(w_grade_status_enrolled_at_plu).alias("grade_status_enrolled_at_plus_7d_to_date_max_sco"),
    )
)

with_parent_course = (
    sequenced.alias("s")
    .join(df_courses.alias("c"), F.col("c.id") == F.col("s.course_id"), "left")
    .select(
        "s.id",
        "s.student_id",
        "s.course_id",
        "s.term",
        "s.score",
        "s.enrolled_at",
        "s.score_filled",
        "s.enrolled_at_plus_7d",
        "s.enrolled_at_seq",
        "s.second_score_filled",
        F.col("c.level").alias("course_level"),
    )
)

enrollment_stats = (
    df_enrollments
    .groupBy("student_id")
    .agg(
        F.count(F.when(F.col("grade_status").isin("FAIL", "INCOMPLETE"), 1)).alias("enrollment_grade_status_count"),
        F.max("enrolled_at").alias("last_enrollment_enrolled_at"),
        F.count("*").alias("enrollment_count"),
    )
)

with_counts = (
    with_parent_course.alias("s")
    .join(enrollment_stats.alias("e"), F.col("e.student_id") == F.col("s.student_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("e.enrollment_grade_status_count"), F.lit(0)).alias("enrollment_grade_status_count"),
        "e.last_enrollment_enrolled_at",
        F.coalesce(F.col("e.enrollment_count"), F.lit(0)).alias("enrollment_count"),
    )
)

enriched = (
    with_counts
    .select(
        "*",
        F.concat_ws("-", F.col("term"), F.col("course_level")).alias("term_course_level_label"),
        F.to_date(F.col("enrolled_at_plus_7d")).alias("enrolled_at_plus_7d_date"),
    )
)

with_parent = (
    enriched.alias("s")
    .join(df_courses.alias("c"), F.col("c.id") == F.col("s.course_id"), "inner")
    .join(df_students.alias("st"), F.col("st.id") == F.col("s.student_id"), "left")
    .select(
        "s.id",
        "s.student_id",
        "s.course_id",
        "s.term",
        "s.score",
        "s.enrolled_at_plus_7d",
        "s.second_score_filled",
        "s.course_level",
        "s.enrollment_grade_status_count",
        "s.last_enrollment_enrolled_at",
        "s.enrollment_count",
        "s.term_course_level_label",
        "s.enrolled_at_plus_7d_date",
        F.col("c.credits").alias("course_credits"),
        "c.course_code",
        F.col("st.enrolled_on").alias("student_enrolled_on"),
    )
)

with_windows = (
    with_parent
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "enrolled_at_plus_7d",
        "second_score_filled",
        "course_level",
        "enrollment_grade_status_count",
        "last_enrollment_enrolled_at",
        "enrollment_count",
        "term_course_level_label",
        "enrolled_at_plus_7d_date",
        "course_credits",
        "course_code",
        "student_enrolled_on",
        F.rank().over(w_term_enrollment_count_by_sec).alias("second_score_filled_rank"),
        F.sum("course_credits").over(w_course_id_enrollment_grade_s).alias("course_credits_sum_3_prior"),
        F.ntile(5).over(w_term_enrollment_count_by_sec).alias("second_score_filled_quintile"),
        F.rank().over(w_enrollment_grade_status_coun).alias("enrolled_at_plus_7d_date_rank"),
    )
)

result = (
    with_windows
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "enrolled_at_plus_7d",
        "course_level",
        "enrollment_grade_status_count",
        "last_enrollment_enrolled_at",
        "enrollment_count",
        "term_course_level_label",
        "course_credits",
        "course_code",
        "second_score_filled_rank",
        "course_credits_sum_3_prior",
        "enrolled_at_plus_7d_date_rank",
        F.date_format(F.col("enrolled_at_plus_7d"), "MMM yyyy").alias("enrolled_at_plus_7d_label"),
    )
    .orderBy("last_enrollment_enrolled_at", "second_score_filled_rank")
)
