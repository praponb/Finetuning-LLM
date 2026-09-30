from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_score = Window.orderBy(F.col("score").desc(), "id")
w_grade_status_term_by_score = Window.partitionBy("grade_status", "term").orderBy(F.col("score").desc(), "id")
w_student_id_course_dept_by_en = Window.partitionBy("student_id", "course_dept").orderBy(F.col("enrolled_at").desc(), "id")
w_term_by_score_running = Window.partitionBy("term").orderBy("score", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_student_id_course_level = Window.partitionBy("student_id", "course_level")
w_term = Window.partitionBy("term")
w_grade_status_term_by_score_r = Window.partitionBy("grade_status", "term").orderBy(F.col("score").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

filtered_enrollments = (
    df_enrollments
    .filter((~F.col("term").isin("2024T2", "2025T2")) & (F.col("course_id").isNotNull()))
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
        "enrolled_at",
        F.upper(F.trim(F.col("grade_status"))).alias("grade_status_clean"),
        F.substring(F.trim(F.col("grade_status")), 1, 2).alias("grade_status_prefix"),
    )
)

with_details = (
    filtered_enrollments.alias("s")
    .join(df_courses.alias("c"), F.col("c.id") == F.col("s.course_id"), "left")
    .join(df_students.alias("st"), F.col("st.id") == F.col("s.student_id"), "inner")
    .select(
        "s.id",
        "s.student_id",
        "s.course_id",
        "s.term",
        "s.score",
        "s.grade_status",
        "s.enrolled_at",
        "s.grade_status_clean",
        F.col("c.dept").alias("course_dept"),
        F.col("c.level").alias("course_level"),
        F.col("c.credits").alias("course_credits"),
        "st.student_name",
    )
)

windowed = (
    with_details
    .select(
        "*",
        F.ntile(10).over(w_all_by_score).alias("score_decile"),
        F.round(F.cume_dist().over(w_grade_status_term_by_score), 3).alias("score_cume_dist"),
        F.row_number().over(w_student_id_course_dept_by_en).alias("enrolled_at_seq"),
        F.count(F.lit(1)).over(w_term_by_score_running).alias("running_count"),
        F.min("course_credits").over(w_student_id_course_level).alias("student_id_course_level_min_course_credits"),
        F.round(F.col("course_credits") / F.nullif(F.sum("course_credits").over(w_student_id_course_level), F.lit(0)), 4).alias("course_credits_share_of_student_id_course_level"),
        (F.col("course_credits") - F.lag("course_credits").over(w_grade_status_term_by_score)).alias("course_credits_change"),
        F.round(F.avg("course_credits").over(w_term), 2).alias("term_avg_course_credits"),
        F.round(F.col("course_credits") / F.nullif(F.sum("course_credits").over(w_term), F.lit(0)), 4).alias("course_credits_share_of_term"),
        F.count(F.lit(1)).over(w_student_id_course_level).alias("rows_in_student_id_course_level"),
        F.round(F.col("score") - F.avg("score").over(w_term), 2).alias("score_vs_term_avg"),
        F.min("course_credits").over(w_grade_status_term_by_score_r).alias("min_course_credits_so_far"),
        F.round(F.percent_rank().over(w_grade_status_term_by_score), 3).alias("score_percent_rank"),
    )
)

totals = (
    windowed
    .groupBy(
        "course_credits_change",
        F.when(F.col("score_percent_rank") >= 0.6, "large").when(F.col("score_percent_rank") >= 0.2, "medium").otherwise("small").alias("score_percent_rank_tier"),
        "enrolled_at_seq",
    )
    .agg(
        F.round(F.stddev("term_avg_course_credits"), 2).alias("stddev_term_avg_course_credits"),
        F.count(F.when(F.length(F.trim(F.col("student_name"))) >= 8, 1)).alias("n_length_trim_student_name_8"),
        F.sum("course_credits").alias("total_course_credits"),
        F.round(F.sum("course_credits_share_of_term") / F.nullif(F.sum("student_id_course_level_min_course_credits"), F.lit(0)), 3).alias("course_credits_share_of_term_to_student_id_cours"),
        F.sum(F.when(F.length(F.trim(F.col("student_name"))) >= 5, 1).otherwise(0)).alias("n_length_trim_student_name_5"),
    )
)

result = (
    totals
    .filter(F.col("course_credits_change") <= 1)
    .select(
        "course_credits_change",
        "score_percent_rank_tier",
        "enrolled_at_seq",
        "stddev_term_avg_course_credits",
        "n_length_trim_student_name_8",
        "total_course_credits",
        "course_credits_share_of_term_to_student_id_cours",
        "n_length_trim_student_name_5",
    )
    .orderBy("total_course_credits", F.col("n_length_trim_student_name_8").desc())
)
