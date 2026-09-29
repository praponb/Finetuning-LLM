from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_enrolled_at_month = Window.partitionBy(F.month("enrolled_at"))
w_enrolled_at_month_by_score_r = Window.partitionBy(F.date_trunc("month", "enrolled_at")).orderBy(F.col("score").desc_nulls_first(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_enrolled_at_month_by_score = Window.partitionBy(F.date_trunc("month", "enrolled_at")).orderBy(F.col("score").desc_nulls_first(), "id")
w_enrolled_at_month_by_score_p = Window.partitionBy(F.date_trunc("month", "enrolled_at")).orderBy(F.col("score").desc_nulls_first(), "id").rowsBetween(-1, -1)
w_course_id = Window.partitionBy("course_id")
w_student_id_grade_status_by_e = Window.partitionBy("student_id", "grade_status").orderBy(F.col("enrolled_at_hour").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_term = Window.partitionBy("term")
w_score_tier_term_by_enrolled = Window.partitionBy("score_tier", "term").orderBy(F.col("enrolled_at_hour").desc_nulls_first())
w_all_by_score_share_of_enroll = Window.orderBy("score_share_of_enrolled_at_month")

valid_enrollments = (
    df_enrollments
    .filter((F.col("course_id").isNotNull()) & (F.col("student_id") >= 15))
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
        "enrolled_at",
        F.date_trunc("hour", F.col("enrolled_at")).alias("enrolled_at_hour"),
        F.when(F.col("score") >= 79.3, "large").when(F.col("score") >= 26.2, "medium").otherwise("small").alias("score_tier"),
    )
)

with_windows = (
    valid_enrollments
    .select(
        "*",
        F.round(F.col("score") / F.nullif(F.sum("score").over(w_enrolled_at_month), F.lit(0)), 4).alias("score_share_of_enrolled_at_month"),
        F.count(F.lit(1)).over(w_enrolled_at_month_by_score_r).alias("running_count"),
        F.dense_rank().over(w_enrolled_at_month_by_score).alias("score_dense_rank"),
        F.count(F.lit(1)).over(w_enrolled_at_month).alias("rows_in_enrolled_at_month"),
        F.round(F.avg("score").over(w_enrolled_at_month), 2).alias("enrolled_at_month_avg_score"),
        F.round(F.avg("score").over(w_enrolled_at_month_by_score_p), 2).alias("score_avg_1_prior"),
    )
)

with_windows_2 = (
    with_windows
    .select(
        "*",
        F.max("score_avg_1_prior").over(w_course_id).alias("course_id_max_score_avg_1_prior"),
        F.nth_value("score", 2).over(w_student_id_grade_status_by_e).alias("second_score"),
        F.nth_value("enrolled_at_month_avg_score", 2).over(w_student_id_grade_status_by_e).alias("second_enrolled_at_month_avg_score"),
    )
)

sequenced = (
    with_windows_2
    .select(
        "*",
        F.count(F.lit(1)).over(w_term).alias("rows_in_term"),
        F.round(F.percent_rank().over(w_score_tier_term_by_enrolled), 3).alias("enrolled_at_hour_percent_rank"),
        F.dense_rank().over(w_all_by_score_share_of_enroll).alias("score_share_of_enrolled_at_month_dense_rank"),
    )
)

with_details = (
    sequenced.alias("s")
    .join(df_courses.alias("c"), F.col("c.id") == F.col("s.course_id"), "inner")
    .join(df_students.alias("st"), F.col("st.id") == F.col("s.student_id"), "left")
    .filter(F.col("c.level") == "core")
    .select(
        "s.*",
        F.col("c.level").alias("course_level"),
        F.col("c.credits").alias("course_credits"),
        "st.student_name",
        F.col("st.enrolled_on").alias("student_enrolled_on"),
    )
)

combined_details = (
    with_details.alias("s")
    .join(df_students.alias("st"), F.col("st.id") == F.col("s.student_id"), "left")
    .join(df_courses.alias("c"), F.col("c.id") == F.col("s.course_id"), "left")
    .select(
        "s.id",
        "s.student_id",
        "s.course_id",
        "s.term",
        "s.grade_status",
        "s.enrolled_at",
        "s.score_tier",
        "s.score_share_of_enrolled_at_month",
        "s.running_count",
        "s.score_dense_rank",
        "s.rows_in_enrolled_at_month",
        "s.enrolled_at_month_avg_score",
        "s.score_avg_1_prior",
        "s.course_id_max_score_avg_1_prior",
        "s.second_score",
        "s.rows_in_term",
        "s.course_level",
        "s.student_name",
        F.col("st.is_international").alias("student_is_international"),
        F.col("st.major").alias("student_major"),
        F.col("c.level").alias("course_level_2"),
        "c.course_code",
    )
)

with_parent_student = (
    combined_details.alias("s")
    .join(df_students.alias("st"), F.col("st.id") == F.col("s.student_id"), "inner")
    .join(df_courses.alias("c"), F.col("c.id") == F.col("s.course_id"), "left")
    .select(
        "s.*",
        F.col("st.student_name").alias("student_student_name"),
        F.col("st.gpa").alias("student_gpa"),
        F.col("st.cohort").alias("student_cohort"),
        F.col("c.credits").alias("course_credits"),
        F.col("c.dept").alias("course_dept"),
    )
)

result = (
    with_parent_student
    .cube("course_credits", "course_level", "student_major")
    .agg(
        F.grouping_id().alias("grouping_level"),
        F.sum("score_share_of_enrolled_at_month").alias("total_score_share_of_enrolled_at_month"),
        F.count("*").alias("n_rows"),
        F.countDistinct("student_cohort").alias("distinct_student_cohorts"),
    )
    .orderBy(
        F.col("course_credits").asc_nulls_last(),
        F.col("course_level").asc_nulls_last(),
        F.col("student_major").asc_nulls_last(),
    )
)
