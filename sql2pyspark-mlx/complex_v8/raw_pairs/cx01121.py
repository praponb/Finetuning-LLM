from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_course_id = Window.partitionBy("course_id")
w_all_by_score_prior_2 = Window.orderBy(F.col("score").desc(), "id").rowsBetween(-2, -1)
w_all_by_score_full = Window.orderBy("score", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_score_running = Window.orderBy("score", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_score_moving_2 = Window.orderBy("score", "id").rowsBetween(-2, Window.currentRow)

scoped_enrollments = (
    df_enrollments
    .filter((F.col("score") >= 6.2) & (F.col("grade_status") != "PASS"))
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
    )
)

without_matches = (
    scoped_enrollments.alias("s")
    .join(df_enrollments.alias("e"), F.col("e.course_id") == F.col("s.course_id"), "left_semi")
)

windowed = (
    without_matches
    .select(
        "*",
        F.sum("score").over(w_course_id).alias("course_id_sum_score"),
        F.max("score").over(w_all_by_score_prior_2).alias("score_max_2_prior"),
        F.nth_value("score", 2).over(w_all_by_score_full).alias("second_score"),
        F.sum("score").over(w_all_by_score_running).alias("running_score"),
        F.sum("score").over(w_all_by_score_moving_2).alias("score_sum_3"),
    )
)

result = (
    windowed
    .rollup("term", "course_id")
    .agg(
        F.grouping_id().alias("gid"),
        F.percentile_approx("course_id_sum_score", 0.5).alias("median_course_id_sum_score"),
        F.max("course_id_sum_score").alias("max_course_id_sum_score"),
        F.sum(F.when(F.col("term") == "2025T1", F.col("score_max_2_prior")).otherwise(0)).alias("score_max_2_prior_term_2025t1"),
    )
    .select(
        F.coalesce(F.col("term"), F.lit("ALL")).alias("term"),
        "course_id",
        "gid",
        "median_course_id_sum_score",
        "max_course_id_sum_score",
        "score_max_2_prior_term_2025t1",
    )
    .orderBy(F.col("term").asc_nulls_last(), F.col("course_id").asc_nulls_last())
)
