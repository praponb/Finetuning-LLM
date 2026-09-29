from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_course_id_enrolled_at_day_ye = Window.partitionBy("course_id", F.year("enrolled_at_day"))
w_distinct_grade_status_enroll = Window.partitionBy("distinct_grade_status", F.month("enrolled_at_day")).orderBy("score_to_course_credits_ratio", "enrolled_at_day", "course_id")
w_distinct_grade_status_enroll_2 = Window.partitionBy("distinct_grade_status", F.month("enrolled_at_day")).orderBy("score_to_course_credits_ratio", "enrolled_at_day", "course_id").rowsBetween(-2, -1)
w_all_by_distinct_grade_status = Window.orderBy("distinct_grade_status", "enrolled_at_day", "course_id")
w_all_by_distinct_grade_status_2 = Window.orderBy(F.col("distinct_grade_status_share_of_course_id_enrolle").asc_nulls_last(), "enrolled_at_day", "course_id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_distinct_grade_status_3 = Window.orderBy(F.col("distinct_grade_status_share_of_course_id_enrolle").asc_nulls_last(), "enrolled_at_day", "course_id").rowsBetween(-1, -1)
w_distinct_grade_status = Window.partitionBy("distinct_grade_status")

recent_enrollments = (
    df_enrollments
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
        "enrolled_at",
        F.upper(F.trim(F.col("term"))).alias("term_clean"),
    )
)

with_details = (
    recent_enrollments.alias("s")
    .join(df_courses.alias("c"), (F.col("c.id") == F.col("s.course_id")) & (F.col("c.course_code").isNotNull()), "inner")
    .join(df_students.alias("st"), F.col("st.id") == F.col("s.student_id"), "left")
    .select(
        "s.id",
        "s.student_id",
        "s.course_id",
        "s.term",
        "s.score",
        "s.grade_status",
        "s.enrolled_at",
        "s.term_clean",
        F.col("c.credits").alias("course_credits"),
        F.col("c.level").alias("course_level"),
        "c.course_code",
        F.col("st.is_international").alias("student_is_international"),
    )
)

per_group = (
    with_details
    .groupBy(F.to_date("enrolled_at").alias("enrolled_at_day"), "course_id")
    .agg(
        F.round(F.sum("score") / F.nullif(F.sum("course_credits"), F.lit(0)), 3).alias("score_to_course_credits_ratio"),
        F.countDistinct("grade_status").alias("distinct_grade_status"),
    )
)

ranked = (
    per_group
    .select(
        "enrolled_at_day",
        "course_id",
        "distinct_grade_status",
        F.round(F.col("distinct_grade_status") / F.nullif(F.sum("distinct_grade_status").over(w_course_id_enrolled_at_day_ye), F.lit(0)), 4).alias("distinct_grade_status_share_of_course_id_enrolle"),
        F.round(F.col("distinct_grade_status") - F.avg("distinct_grade_status").over(w_course_id_enrolled_at_day_ye), 2).alias("distinct_grade_status_vs_course_id_enrolled_at_d"),
        F.lag("enrolled_at_day", 3).over(w_distinct_grade_status_enroll).alias("prev3_enrolled_at_day"),
        F.max("distinct_grade_status").over(w_distinct_grade_status_enroll_2).alias("distinct_grade_status_max_2_prior"),
        F.sum("distinct_grade_status").over(w_course_id_enrolled_at_day_ye).alias("course_id_enrolled_at_day_year_sum_distinct_grad"),
        F.round(F.percent_rank().over(w_all_by_distinct_grade_status), 3).alias("distinct_grade_status_percent_rank"),
    )
)

enrollment_stats = (
    df_enrollments
    .groupBy("course_id")
    .agg(
        F.count(F.when(F.col("grade_status").isin("WITHDRAWN", "PASS"), 1)).alias("enrollment_grade_status_count"),
        F.max("enrolled_at").alias("last_enrollment_enrolled_at"),
        F.max("score").alias("max_enrollment_score"),
    )
)

with_counts = (
    ranked.alias("s")
    .join(enrollment_stats.alias("e"), F.col("e.course_id") == F.col("s.course_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("e.enrollment_grade_status_count"), F.lit(0)).alias("enrollment_grade_status_count"),
        "e.last_enrollment_enrolled_at",
        "e.max_enrollment_score",
    )
)

windowed = (
    with_counts
    .select(
        "enrolled_at_day",
        "course_id",
        "distinct_grade_status",
        "distinct_grade_status_share_of_course_id_enrolle",
        "distinct_grade_status_vs_course_id_enrolled_at_d",
        "prev3_enrolled_at_day",
        "distinct_grade_status_percent_rank",
        "enrollment_grade_status_count",
        F.nth_value("distinct_grade_status", 2).over(w_all_by_distinct_grade_status_2).alias("second_distinct_grade_status"),
        F.round(F.avg("distinct_grade_status").over(w_all_by_distinct_grade_status_3), 2).alias("distinct_grade_status_avg_1_prior"),
        F.size(F.collect_set("course_id").over(w_distinct_grade_status)).alias("distinct_course_id_in_distinct_grade_status"),
        F.count(F.lit(1)).over(w_distinct_grade_status).alias("rows_in_distinct_grade_status"),
        F.round(F.avg("distinct_grade_status").over(w_distinct_grade_status), 2).alias("distinct_grade_status_avg_distinct_grade_status"),
    )
)

result = (
    windowed
    .filter(F.year(F.col("enrolled_at_day")) == 2024)
    .select(
        "enrolled_at_day",
        "course_id",
        "distinct_grade_status_share_of_course_id_enrolle",
        "prev3_enrolled_at_day",
        "distinct_grade_status_percent_rank",
        "second_distinct_grade_status",
        "distinct_grade_status_avg_1_prior",
        "distinct_course_id_in_distinct_grade_status",
        "rows_in_distinct_grade_status",
        "distinct_grade_status_avg_distinct_grade_status",
        F.datediff(F.col("prev3_enrolled_at_day"), F.col("enrolled_at_day")).alias("days_enrolled_at_day_to_prev3_enrolled_at_day"),
    )
)
