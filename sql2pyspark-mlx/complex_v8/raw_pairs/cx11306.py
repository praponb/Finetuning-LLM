from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_student_major_course_id_by_s = Window.partitionBy("student_major", "course_id").orderBy(F.col("score").desc(), "id")
w_all_by_score = Window.orderBy(F.col("score").asc_nulls_last(), "id")
w_all_by_course_credits_full = Window.orderBy(F.col("course_credits").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_student_id_student_major = Window.partitionBy("student_id", "student_major")
w_student_major_course_id_by_s_2 = Window.partitionBy("student_major", "course_id").orderBy(F.col("score").desc())
w_all_by_score_full = Window.orderBy(F.col("score").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_course_credits_runnin = Window.orderBy(F.col("course_credits").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_student_id_course_credits_by = Window.partitionBy("student_id", "course_credits").orderBy("second_score", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_student_id_score_tercile = Window.partitionBy("student_id", "score_tercile")
w_all_by_course_credits_prior = Window.orderBy(F.col("course_credits").desc(), "id").rowsBetween(-3, -1)
w_student_id_course_credits_by_2 = Window.partitionBy("student_id", "course_credits").orderBy("second_score")
w_student_id_course_credits_by_3 = Window.partitionBy("student_id", "course_credits").orderBy("second_score", "id").rowsBetween(-2, Window.currentRow)

recent_enrollments = (
    df_enrollments
    .filter(~F.col("term").isin("2025T1", "2024T1"))
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
    )
)

with_details_student = (
    recent_enrollments.alias("s")
    .join(df_students.alias("st"), F.col("st.id") == F.col("s.student_id"), "inner")
    .join(df_courses.alias("c"), F.col("c.id") == F.col("s.course_id"), "left")
    .filter(F.col("st.is_international"))
    .select(
        "s.id",
        "s.student_id",
        "s.course_id",
        "s.term",
        "s.score",
        "s.grade_status",
        F.col("st.is_international").alias("student_is_international"),
        F.col("st.major").alias("student_major"),
        F.col("c.dept").alias("course_dept"),
        F.col("c.credits").alias("course_credits"),
    )
)

windowed = (
    with_details_student
    .select(
        "*",
        F.round(F.cume_dist().over(w_student_major_course_id_by_s), 3).alias("score_cume_dist"),
        F.lead("score", 3).over(w_all_by_score).alias("next3_score"),
        F.nth_value("score", 2).over(w_all_by_course_credits_full).alias("second_score"),
        F.round(F.col("score") / F.nullif(F.sum("score").over(w_student_id_student_major), F.lit(0)), 4).alias("score_share_of_student_id_student_major"),
        F.size(F.collect_set("term").over(w_student_id_student_major)).alias("distinct_term_in_student_id_student_major"),
        F.ntile(3).over(w_student_major_course_id_by_s).alias("score_tercile"),
        F.ntile(5).over(w_student_major_course_id_by_s).alias("score_quintile"),
        F.max("course_credits").over(w_student_id_student_major).alias("student_id_student_major_max_course_credits"),
        F.dense_rank().over(w_student_major_course_id_by_s_2).alias("score_dense_rank"),
        F.size(F.collect_set("grade_status").over(w_student_id_student_major)).alias("distinct_grade_status_in_student_id_student_majo"),
        F.nth_value("course_credits", 2).over(w_all_by_score_full).alias("second_course_credits"),
        F.sum("score").over(w_all_by_course_credits_runnin).alias("running_score"),
    )
)

with_windows = (
    windowed
    .select(
        "*",
        F.last("score_dense_rank", ignorenulls=True).over(w_student_id_course_credits_by).alias("last_known_score_dense_rank"),
        F.round(F.col("score_dense_rank") - F.avg("score_dense_rank").over(w_student_id_score_tercile), 2).alias("score_dense_rank_vs_student_id_score_tercile_avg"),
        F.round(F.avg("running_score").over(w_all_by_course_credits_prior), 2).alias("running_score_avg_3_prior"),
        F.round(F.col("score_tercile") - F.avg("score_tercile").over(w_student_id_score_tercile), 2).alias("score_tercile_vs_student_id_score_tercile_avg"),
        F.round(F.cume_dist().over(w_student_id_course_credits_by_2), 3).alias("second_score_cume_dist"),
        F.size(F.collect_set("course_dept").over(w_student_id_score_tercile)).alias("distinct_course_dept_in_student_id_score_tercile"),
        F.round(F.avg("score_dense_rank").over(w_student_id_course_credits_by_3), 2).alias("score_dense_rank_avg_3"),
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
        "grade_status",
        "student_is_international",
        "student_major",
        "course_credits",
        "score_cume_dist",
        "next3_score",
        "score_share_of_student_id_student_major",
        "distinct_term_in_student_id_student_major",
        "score_tercile",
        "student_id_student_major_max_course_credits",
        "distinct_grade_status_in_student_id_student_majo",
        "second_course_credits",
        "running_score",
        "last_known_score_dense_rank",
        "running_score_avg_3_prior",
        "distinct_course_dept_in_student_id_score_tercile",
        "score_dense_rank_avg_3",
        F.coalesce(F.col("score"), F.lit(0.0)).alias("score_filled"),
        F.abs(F.col("score_dense_rank") - 6).alias("score_dense_rank_distance"),
    )
    .orderBy(F.col("score_cume_dist").desc(), F.col("score_filled").desc())
)
