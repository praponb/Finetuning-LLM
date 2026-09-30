from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_course_id_grade_status_by_sc = Window.partitionBy("course_id", "grade_status").orderBy(F.col("score_distance").desc(), "id")
w_course_id_term_by_score = Window.partitionBy("course_id", "term").orderBy(F.col("score").desc(), "id")
w_grade_status = Window.partitionBy("grade_status")
w_score_distance_dense_rank_st = Window.partitionBy("score_distance_dense_rank", "student_id").orderBy("rows_in_grade_status", "id")
w_all_by_next_score_prior_6 = Window.orderBy("next_score", "id").rowsBetween(-6, -1)
w_course_id = Window.partitionBy("course_id")
w_score_distance_dense_rank_st_2 = Window.partitionBy("score_distance_dense_rank", "student_id").orderBy("rows_in_grade_status", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

prepared = (
    df_enrollments
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
        "enrolled_at",
        F.abs(F.col("score") - 34.2).alias("score_distance"),
    )
)

with_windows = (
    prepared
    .select(
        "*",
        F.round((F.unix_timestamp("enrolled_at") - F.unix_timestamp(F.lag("enrolled_at").over(w_course_id_grade_status_by_sc))) / 60, 1).alias("minutes_since_prev_enrolled_at"),
        F.dense_rank().over(w_course_id_grade_status_by_sc).alias("score_distance_dense_rank"),
        F.lead("score").over(w_course_id_term_by_score).alias("next_score"),
        F.count(F.lit(1)).over(w_grade_status).alias("rows_in_grade_status"),
    )
)

windowed = (
    with_windows
    .select(
        "id",
        "student_id",
        "score",
        "grade_status",
        "enrolled_at",
        "score_distance",
        "score_distance_dense_rank",
        "next_score",
        "rows_in_grade_status",
        F.lead("enrolled_at", 3).over(w_score_distance_dense_rank_st).alias("next3_enrolled_at"),
        F.max("score_distance_dense_rank").over(w_all_by_next_score_prior_6).alias("score_distance_dense_rank_max_6_prior"),
        F.count(F.lit(1)).over(w_course_id).alias("rows_in_course_id"),
        F.row_number().over(w_score_distance_dense_rank_st).alias("rn"),
        F.sum("next_score").over(w_score_distance_dense_rank_st_2).alias("running_next_score"),
        F.size(F.collect_set("grade_status").over(w_course_id)).alias("distinct_grade_status_in_course_id"),
    )
)

enrollment_stats = (
    df_enrollments
    .groupBy("student_id")
    .agg(
        F.count(F.when(F.col("term").isin("2025T1", "2025T2"), 1)).alias("enrollment_term_count"),
    )
)

profiled = (
    windowed.alias("s")
    .join(enrollment_stats.alias("e"), F.col("e.student_id") == F.col("s.student_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("e.enrollment_term_count"), F.lit(0)).alias("enrollment_term_count"),
    )
)

result = (
    profiled
    .select(
        "id",
        "student_id",
        "grade_status",
        "enrolled_at",
        "score_distance_dense_rank",
        "next3_enrolled_at",
        "rows_in_course_id",
        "rn",
        "running_next_score",
        "distinct_grade_status_in_course_id",
        F.coalesce(F.col("next_score"), F.lit(0.0)).alias("next_score_filled"),
    )
)
