from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_grade_status_course_id = Window.partitionBy("grade_status", "course_id")
w_all_by_score_prior_3 = Window.orderBy(F.col("score").desc(), "id").rowsBetween(-3, -1)
w_enrolled_at_date_year = Window.partitionBy(F.year("enrolled_at_date"))
w_all_by_score = Window.orderBy(F.col("score").desc(), "id")
w_all_by_score_full = Window.orderBy(F.col("score").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_course_id_grade_status_by_en = Window.partitionBy("course_id", "grade_status").orderBy("enrolled_at_date")
w_all_by_student_gpa_running = Window.orderBy("student_gpa", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_score_ties = Window.orderBy(F.col("score").desc())
w_all_by_student_gpa_ties = Window.orderBy("student_gpa")
w_enrolled_at_date_month_by_en = Window.partitionBy(F.month("enrolled_at_date")).orderBy("enrolled_at_date", "id")
w_distinct_grade_status_in_enr = Window.partitionBy("distinct_grade_status_in_enrolled_at_date_year", "score_seq")
w_distinct_course_id_in_enroll = Window.partitionBy("distinct_course_id_in_enrolled_at_date_year").orderBy("first_score", "id")
w_all_by_enrolled_at_date_dens = Window.orderBy("enrolled_at_date_dense_rank", "id")
w_enrolled_at_date_quintile_by = Window.partitionBy("enrolled_at_date_quintile").orderBy(F.col("enrolled_at_date_year_max_score").desc(), "id")
w_student_id_first_score_quint = Window.partitionBy("student_id", "first_score_quintile")

valid_enrollments = (
    df_enrollments
    .filter((F.col("score").between(4.9, 69.6)) & (F.col("grade_status").isNotNull()))
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
        F.to_date(F.col("enrolled_at")).alias("enrolled_at_date"),
    )
)

combined_details = (
    valid_enrollments.alias("s")
    .join(df_students.alias("st"), (F.col("st.id") == F.col("s.student_id")) & (F.col("st.enrolled_on") < F.lit("2025-05-07").cast("date")), "inner")
    .join(df_courses.alias("c"), F.col("c.id") == F.col("s.course_id"), "left")
    .filter(F.col("st.gpa") <= 3.2)
    .select(
        "s.id",
        "s.student_id",
        "s.course_id",
        "s.score",
        "s.grade_status",
        "s.enrolled_at_date",
        F.col("st.gpa").alias("student_gpa"),
        "c.course_code",
    )
)

metrics = (
    combined_details
    .select(
        "*",
        F.count(F.lit(1)).over(w_grade_status_course_id).alias("rows_in_grade_status_course_id"),
        F.round(F.avg("score").over(w_all_by_score_prior_3), 2).alias("score_avg_3_prior"),
        F.size(F.collect_set("course_id").over(w_enrolled_at_date_year)).alias("distinct_course_id_in_enrolled_at_date_year"),
        F.row_number().over(w_all_by_score).alias("score_seq"),
        F.size(F.collect_set("grade_status").over(w_enrolled_at_date_year)).alias("distinct_grade_status_in_enrolled_at_date_year"),
        F.round(F.avg("student_gpa").over(w_grade_status_course_id), 2).alias("grade_status_course_id_avg_student_gpa"),
        F.last("student_gpa").over(w_all_by_score_full).alias("last_student_gpa"),
        F.round(F.col("student_gpa") - F.avg("student_gpa").over(w_grade_status_course_id), 2).alias("student_gpa_vs_grade_status_course_id_avg"),
        F.dense_rank().over(w_course_id_grade_status_by_en).alias("enrolled_at_date_dense_rank"),
        F.max("score").over(w_enrolled_at_date_year).alias("enrolled_at_date_year_max_score"),
        F.first("student_gpa", ignorenulls=True).over(w_all_by_student_gpa_running).alias("first_known_student_gpa"),
        F.round(F.percent_rank().over(w_all_by_score_ties), 3).alias("score_percent_rank"),
        F.round(F.cume_dist().over(w_all_by_student_gpa_ties), 3).alias("student_gpa_cume_dist"),
        F.ntile(5).over(w_enrolled_at_date_month_by_en).alias("enrolled_at_date_quintile"),
        F.first("score").over(w_all_by_score_full).alias("first_score"),
    )
)

sequenced = (
    metrics
    .select(
        "*",
        F.round(F.col("score_avg_3_prior") - F.avg("score_avg_3_prior").over(w_distinct_grade_status_in_enr), 2).alias("score_avg_3_prior_vs_distinct_grade_status_in_en"),
        F.ntile(5).over(w_distinct_course_id_in_enroll).alias("first_score_quintile"),
        F.lag("score_percent_rank").over(w_all_by_enrolled_at_date_dens).alias("prev_score_percent_rank"),
        F.lag("score_seq").over(w_enrolled_at_date_quintile_by).alias("prev_score_seq"),
    )
)

totals = (
    sequenced
    .groupBy("student_id", "first_score_quintile")
    .agg(
        F.sum(F.when(F.length(F.trim(F.col("course_code"))) >= 7, F.col("score_percent_rank"))).alias("score_percent_rank_length_trim_course_code_7"),
        F.round(F.sum("grade_status_course_id_avg_student_gpa") / F.nullif(F.sum("student_gpa"), F.lit(0)), 3).alias("grade_status_course_id_avg_student_gpa_to_studen"),
        F.datediff(F.max("enrolled_at_date"), F.min("enrolled_at_date")).alias("enrolled_at_date_span_days"),
        F.percentile_approx("student_gpa", 0.5).alias("median_student_gpa"),
        F.max("grade_status_course_id_avg_student_gpa").alias("max_grade_status_course_id_avg_student_gpa_check"),
    )
    .filter(F.col("max_grade_status_course_id_avg_student_gpa_check") > 2.1)
    .drop("max_grade_status_course_id_avg_student_gpa_check")
)

windowed = (
    totals
    .select(
        "student_id",
        "first_score_quintile",
        "median_student_gpa",
        F.max("first_score_quintile").over(w_student_id_first_score_quint).alias("student_id_first_score_quintile_max_first_score_"),
        F.count(F.lit(1)).over(w_student_id_first_score_quint).alias("rows_in_student_id_first_score_quintile"),
    )
)

result = (
    windowed
    .select(
        "student_id",
        "student_id_first_score_quintile_max_first_score_",
        "rows_in_student_id_first_score_quintile",
        F.abs(F.col("first_score_quintile") - 5).alias("first_score_quintile_distance"),
    )
    .orderBy(F.col("student_id").desc(), "first_score_quintile_distance")
)
