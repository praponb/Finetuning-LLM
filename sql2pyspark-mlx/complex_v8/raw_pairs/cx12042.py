from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_course_dept_term_by_course_c = Window.partitionBy("course_dept", "term").orderBy("course_credits", "id").rowsBetween(-6, -1)
w_course_credits_course_dept_b = Window.partitionBy("course_credits", "course_dept").orderBy("course_credits", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_term_by_course_credits_runni = Window.partitionBy("term").orderBy(F.col("course_credits").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_student_id_course_dept = Window.partitionBy("student_id", "course_dept")
w_course_dept_term_by_course_c_2 = Window.partitionBy("course_dept", "term").orderBy("course_credits")
w_all_by_score_ties = Window.orderBy("score")
w_term_by_score = Window.partitionBy("term").orderBy("score", "id")
w_course_level = Window.partitionBy("course_level")
w_course_credits_course_dept_b_2 = Window.partitionBy("course_credits", "course_dept").orderBy("course_credits")
w_term_by_score_running = Window.partitionBy("term").orderBy("score", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_term_by_course_credits_prior = Window.partitionBy("term").orderBy(F.col("course_credits").desc(), "id").rowsBetween(-1, -1)
w_course_level_by_score_max_6 = Window.partitionBy("course_level").orderBy(F.col("score_max_6_prior").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_course_credits_course_id_by = Window.partitionBy("course_credits", "course_id").orderBy(F.col("score_rank").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_term = Window.partitionBy("term")
w_course_level_by_score_max_6_2 = Window.partitionBy("course_level").orderBy(F.col("score_max_6_prior").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_score_cume_dist_runni = Window.orderBy("score_cume_dist", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_course_credits_dense_rank_2 = Window.partitionBy("course_credits_dense_rank_2", "course_dept").orderBy("course_credits", "id")

filtered_enrollments = (
    df_enrollments
    .filter((F.col("student_id").isNotNull()) & (F.col("grade_status") == "INCOMPLETE"))
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
        "enrolled_at",
    )
)

with_details = (
    filtered_enrollments.alias("s")
    .join(df_courses.alias("c"), F.col("c.id") == F.col("s.course_id"), "left")
    .select(
        "s.id",
        "s.student_id",
        "s.course_id",
        "s.term",
        "s.score",
        F.col("c.credits").alias("course_credits"),
        F.col("c.dept").alias("course_dept"),
        F.col("c.level").alias("course_level"),
    )
)

ranked = (
    with_details
    .select(
        "*",
        F.max("score").over(w_course_dept_term_by_course_c).alias("score_max_6_prior"),
        F.last("course_credits").over(w_course_credits_course_dept_b).alias("last_course_credits"),
        F.max("course_credits").over(w_term_by_course_credits_runni).alias("max_course_credits_so_far"),
        F.round(F.col("course_credits") - F.avg("course_credits").over(w_student_id_course_dept), 2).alias("course_credits_vs_student_id_course_dept_avg"),
        F.dense_rank().over(w_course_dept_term_by_course_c_2).alias("course_credits_dense_rank"),
        F.sum("course_credits").over(w_student_id_course_dept).alias("student_id_course_dept_sum_course_credits"),
        F.rank().over(w_all_by_score_ties).alias("score_rank"),
        F.round(F.cume_dist().over(w_term_by_score), 3).alias("score_cume_dist"),
        F.max("score").over(w_student_id_course_dept).alias("student_id_course_dept_max_score"),
        (F.col("score") - F.lag("score").over(w_term_by_score)).alias("score_change"),
        F.count(F.lit(1)).over(w_course_level).alias("rows_in_course_level"),
        F.dense_rank().over(w_course_credits_course_dept_b_2).alias("course_credits_dense_rank_2"),
        F.round(F.col("score") - F.avg("score").over(w_student_id_course_dept), 2).alias("score_vs_student_id_course_dept_avg"),
        F.sum("score").over(w_term_by_score_running).alias("running_score"),
        F.max("score").over(w_term_by_course_credits_prior).alias("score_max_1_prior"),
    )
)

metrics = (
    ranked
    .select(
        "id",
        "student_id",
        "term",
        "score",
        "score_max_6_prior",
        "last_course_credits",
        "max_course_credits_so_far",
        "course_credits_vs_student_id_course_dept_avg",
        "score_cume_dist",
        "score_change",
        "score_vs_student_id_course_dept_avg",
        "running_score",
        F.first("course_credits_dense_rank_2", ignorenulls=True).over(w_course_level_by_score_max_6).alias("first_known_course_credits_dense_rank_2"),
        F.nth_value("score_change", 2).over(w_course_credits_course_id_by).alias("second_score_change"),
        F.size(F.collect_set("course_dept").over(w_term)).alias("distinct_course_dept_in_term"),
        F.nth_value("score_rank", 2).over(w_course_level_by_score_max_6_2).alias("second_score_rank"),
        F.last("score_change", ignorenulls=True).over(w_all_by_score_cume_dist_runni).alias("last_known_score_change"),
        F.round(F.cume_dist().over(w_course_credits_dense_rank_2), 3).alias("course_credits_cume_dist"),
        F.rank().over(w_course_credits_dense_rank_2).alias("course_credits_rank"),
    )
)

result = (
    metrics
    .select(
        "id",
        "student_id",
        "term",
        "score",
        "last_course_credits",
        "max_course_credits_so_far",
        "course_credits_vs_student_id_course_dept_avg",
        "score_cume_dist",
        "score_change",
        "running_score",
        "second_score_change",
        "second_score_rank",
        "course_credits_rank",
        F.when(F.col("last_known_score_change") >= 70.0, "HIGH").when(F.col("last_known_score_change") >= -20.0, "MEDIUM").otherwise("LOW").alias("last_known_score_change_tier"),
    )
    .orderBy(F.col("score_change").asc_nulls_last())
)
