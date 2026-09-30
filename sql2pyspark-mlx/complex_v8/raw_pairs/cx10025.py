from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_student_id_grade_status = Window.partitionBy("student_id", "grade_status")
w_enrolled_at_day_month_studen = Window.partitionBy(F.month("enrolled_at_day"), "student_id").orderBy(F.col("enrolled_at_day").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_course_id_term_by_score_full = Window.partitionBy("course_id", "term").orderBy(F.col("score").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_score_running = Window.orderBy(F.col("score").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_median_max_score_so_f = Window.orderBy("median_max_score_so_far", "id").rowsBetween(-3, Window.currentRow)
w_all_by_median_max_score_so_f_2 = Window.orderBy("median_max_score_so_far", "id").rowsBetween(-2, Window.currentRow)
w_all_by_distinct_course_id_in = Window.orderBy(F.col("distinct_course_id_in_student_id_grade_status").desc(), "id")
w_all_by_distinct_course_id_in_2 = Window.orderBy(F.col("distinct_course_id_in_student_id_grade_status").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

prepared_enrollments = (
    df_enrollments
    .filter((F.col("enrolled_at") >= F.lit("2025-02-06 11:00:00").cast("timestamp")) & (F.col("course_id").isNotNull()))
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
        F.date_trunc("day", F.col("enrolled_at")).alias("enrolled_at_day"),
    )
)

sequenced = (
    prepared_enrollments
    .select(
        "*",
        F.round(F.col("score") / F.nullif(F.sum("score").over(w_student_id_grade_status), F.lit(0)), 4).alias("score_share_of_student_id_grade_status"),
        F.first("enrolled_at_day").over(w_enrolled_at_day_month_studen).alias("first_enrolled_at_day"),
        F.first("score").over(w_course_id_term_by_score_full).alias("first_score"),
        F.size(F.collect_set("course_id").over(w_student_id_grade_status)).alias("distinct_course_id_in_student_id_grade_status"),
        F.max("score").over(w_all_by_score_running).alias("max_score_so_far"),
        F.first("score", ignorenulls=True).over(w_all_by_score_running).alias("first_known_score"),
    )
)

without_matches = (
    sequenced.alias("s")
    .join(df_enrollments.alias("e"), (F.col("e.student_id") == F.col("s.student_id")) & (~F.col("e.term").isin("2025T2", "2024T2")) & (F.hour(F.col("e.enrolled_at")).between(9, 13)), "left_semi")
    .filter(F.col("student_id").isNotNull())
)

student_id_stats = (
    without_matches
    .groupBy("student_id")
    .agg(F.percentile_approx("max_score_so_far", 0.5).alias("median_max_score_so_far"))
)

above_average = (
    without_matches.alias("s")
    .join(student_id_stats.alias("g"), F.col("g.student_id") == F.col("s.student_id"), "inner")
    .filter(F.col("s.max_score_so_far") >= F.col("g.median_max_score_so_far"))
    .select(
        "s.*",
        "g.median_max_score_so_far",
        F.round(F.col("s.max_score_so_far") - F.col("g.median_max_score_so_far"), 2).alias("max_score_so_far_minus_median"),
    )
)

with_windows = (
    above_average
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
        "score_share_of_student_id_grade_status",
        "first_enrolled_at_day",
        "distinct_course_id_in_student_id_grade_status",
        "max_score_so_far_minus_median",
        F.round(F.avg("max_score_so_far").over(w_all_by_median_max_score_so_f), 2).alias("max_score_so_far_avg_4"),
        F.round(F.avg("median_max_score_so_far").over(w_all_by_median_max_score_so_f_2), 2).alias("median_max_score_so_far_avg_3"),
        F.rank().over(w_all_by_distinct_course_id_in).alias("distinct_course_id_in_student_id_grade_status_ra"),
        F.first("first_score").over(w_all_by_distinct_course_id_in_2).alias("first_first_score"),
    )
)

enriched_course = (
    with_windows.alias("s")
    .join(df_courses.alias("c"), F.col("c.id") == F.col("s.course_id"), "left")
    .join(df_students.alias("st"), F.col("st.id") == F.col("s.student_id"), "left")
    .select(
        "s.id",
        "s.student_id",
        "s.course_id",
        "s.term",
        "s.score",
        "s.grade_status",
        "s.first_enrolled_at_day",
        "s.max_score_so_far_minus_median",
        "s.max_score_so_far_avg_4",
        "s.median_max_score_so_far_avg_3",
        "s.distinct_course_id_in_student_id_grade_status_ra",
        "s.first_first_score",
        F.col("c.credits").alias("course_credits"),
        F.col("st.cohort").alias("student_cohort"),
    )
)

enrollment_stats = (
    df_enrollments
    .groupBy("course_id")
    .agg(F.min("score").alias("min_enrollment_score"))
)

profiled = (
    enriched_course.alias("s")
    .join(enrollment_stats.alias("e"), F.col("e.course_id") == F.col("s.course_id"), "left")
    .select("s.*", "e.min_enrollment_score")
)

result = (
    profiled
    .select(
        "id",
        "course_id",
        "term",
        "score",
        "grade_status",
        "first_enrolled_at_day",
        "max_score_so_far_minus_median",
        "max_score_so_far_avg_4",
        "median_max_score_so_far_avg_3",
        "distinct_course_id_in_student_id_grade_status_ra",
        "first_first_score",
        "course_credits",
        "student_cohort",
        "min_enrollment_score",
        F.coalesce(F.col("grade_status"), F.lit("UNKNOWN")).alias("grade_status_or_unknown"),
        F.when(F.col("first_first_score") >= 59.7, "large").when(F.col("first_first_score") >= 26.2, "medium").otherwise("small").alias("first_first_score_tier"),
    )
    .orderBy(F.col("id").desc(), F.col("grade_status_or_unknown").desc_nulls_first())
)
