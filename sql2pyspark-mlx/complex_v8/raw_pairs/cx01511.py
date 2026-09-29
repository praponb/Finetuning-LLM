from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_grade_status_student_cohort = Window.partitionBy("grade_status", "student_cohort")
w_student_cohort_by_enrolled_a = Window.partitionBy("student_cohort").orderBy("enrolled_at", "id")
w_course_level_student_cohort = Window.partitionBy("course_level", "student_cohort")
w_course_level_grade_status_by = Window.partitionBy("course_level", "grade_status").orderBy(F.col("enrolled_at").desc(), "id")
w_enrolled_at_to_date_student = Window.partitionBy(F.to_date("enrolled_at"), "student_cohort").orderBy("student_gpa")
w_course_level_grade_status_by_2 = Window.partitionBy("course_level", "grade_status").orderBy(F.col("enrolled_at").desc(), "id").rowsBetween(-2, Window.currentRow)
w_all_by_score_adj_vs_course_l = Window.orderBy(F.col("score_adj_vs_course_level_student_cohort_avg").asc_nulls_last(), "id")
w_all_by_score_adj = Window.orderBy("score_adj", "id")
w_student_id_course_id = Window.partitionBy("student_id", "course_id")
w_student_id_by_course_level_s = Window.partitionBy("student_id").orderBy("course_level_student_cohort_sum_student_gpa_to_e", "student_id", "course_level")
w_student_id_by_course_level_s_2 = Window.partitionBy("student_id").orderBy("course_level_student_cohort_sum_student_gpa_to_e", "student_id", "course_level").rowsBetween(Window.unboundedPreceding, Window.currentRow)

valid = (
    df_enrollments
    .filter(~F.col("term").isin("2025T1", "2024T2"))
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
        "enrolled_at",
        F.round(F.col("score") * 1.5, 2).alias("score_adj"),
        F.lower(F.regexp_replace(F.trim(F.col("term")), " ", "_")).alias("term_slug"),
    )
)

with_parent_course = (
    valid.alias("s")
    .join(df_courses.alias("c"), F.col("c.id") == F.col("s.course_id"), "left")
    .join(df_students.alias("st"), F.col("st.id") == F.col("s.student_id"), "left")
    .select(
        "s.id",
        "s.student_id",
        "s.course_id",
        "s.grade_status",
        "s.enrolled_at",
        "s.score_adj",
        "s.term_slug",
        "c.course_code",
        F.col("c.level").alias("course_level"),
        F.col("st.gpa").alias("student_gpa"),
        F.col("st.cohort").alias("student_cohort"),
    )
)

sequenced = (
    with_parent_course
    .select(
        "*",
        F.min("student_gpa").over(w_grade_status_student_cohort).alias("grade_status_student_cohort_min_student_gpa"),
        F.round(F.percent_rank().over(w_student_cohort_by_enrolled_a), 3).alias("enrolled_at_percent_rank"),
        F.sum("student_gpa").over(w_course_level_student_cohort).alias("course_level_student_cohort_sum_student_gpa"),
        F.round(F.col("score_adj") - F.avg("score_adj").over(w_course_level_student_cohort), 2).alias("score_adj_vs_course_level_student_cohort_avg"),
        F.min("score_adj").over(w_course_level_student_cohort).alias("course_level_student_cohort_min_score_adj"),
        F.round(F.cume_dist().over(w_course_level_grade_status_by), 3).alias("enrolled_at_cume_dist"),
        F.dense_rank().over(w_enrolled_at_to_date_student).alias("student_gpa_dense_rank"),
        F.max("student_gpa").over(w_course_level_grade_status_by_2).alias("student_gpa_max_3"),
    )
)

with_trends = (
    sequenced
    .select(
        "id",
        "student_id",
        "course_id",
        "grade_status",
        "score_adj",
        "term_slug",
        "course_code",
        "course_level",
        "student_gpa",
        "student_cohort",
        "enrolled_at_percent_rank",
        "course_level_student_cohort_sum_student_gpa",
        "score_adj_vs_course_level_student_cohort_avg",
        "course_level_student_cohort_min_score_adj",
        "enrolled_at_cume_dist",
        "student_gpa_dense_rank",
        "student_gpa_max_3",
        F.ntile(10).over(w_all_by_score_adj_vs_course_l).alias("score_adj_vs_course_level_student_cohort_avg_dec"),
        F.lag("student_gpa_max_3").over(w_all_by_score_adj).alias("prev_student_gpa_max_3"),
        F.round(F.col("student_gpa") - F.avg("student_gpa").over(w_student_id_course_id), 2).alias("student_gpa_vs_student_id_course_id_avg"),
    )
)

stats = (
    with_trends
    .groupBy("student_id", "course_level")
    .agg(
        F.sum("score_adj_vs_course_level_student_cohort_avg_dec").alias("total_score_adj_vs_course_level_student_cohort_a"),
        F.count("*").alias("row_count"),
        F.round(F.sum("student_gpa_max_3") / F.nullif(F.sum("enrolled_at_percent_rank"), F.lit(0)), 3).alias("student_gpa_max_3_to_enrolled_at_percent_rank_ra"),
        F.countDistinct("id").alias("distinct_ids"),
        F.round(F.sum("course_level_student_cohort_sum_student_gpa") / F.nullif(F.sum("enrolled_at_percent_rank"), F.lit(0)), 3).alias("course_level_student_cohort_sum_student_gpa_to_e"),
    )
    .filter(F.col("row_count") >= 2)
)

windowed = (
    stats
    .select(
        "*",
        F.ntile(5).over(w_student_id_by_course_level_s).alias("course_level_student_cohort_sum_student_gpa_to_e_2"),
        F.ntile(10).over(w_student_id_by_course_level_s).alias("course_level_student_cohort_sum_student_gpa_to_e_3"),
        F.last("distinct_ids", ignorenulls=True).over(w_student_id_by_course_level_s_2).alias("last_known_distinct_ids"),
    )
)

result = (
    windowed
    .select(
        "student_id",
        "course_level",
        "total_score_adj_vs_course_level_student_cohort_a",
        "student_gpa_max_3_to_enrolled_at_percent_rank_ra",
        "distinct_ids",
        "course_level_student_cohort_sum_student_gpa_to_e",
        "course_level_student_cohort_sum_student_gpa_to_e_2",
        "last_known_distinct_ids",
    )
    .orderBy(
        "total_score_adj_vs_course_level_student_cohort_a",
        F.col("distinct_ids").asc(),
    )
)
