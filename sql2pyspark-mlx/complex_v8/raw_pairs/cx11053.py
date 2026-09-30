from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_score_distance_runnin = Window.orderBy("score_distance", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_student_id_grade_status = Window.partitionBy("student_id", "grade_status")
w_all_by_score_distance_ties = Window.orderBy("score_distance")
w_all_by_score_distance = Window.orderBy("score_distance", "id")
w_all_by_student_id_grade_stat = Window.orderBy(F.col("student_id_grade_status_min_score").asc_nulls_last(), "id")
w_grade_status_by_student_id_g = Window.partitionBy("grade_status").orderBy(F.col("student_id_grade_status_min_score").desc(), "id")
w_grade_status_student_id_by_e = Window.partitionBy("grade_status", "student_id").orderBy("enrolled_at", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_student_id = Window.partitionBy("student_id")
w_grade_status = Window.partitionBy("grade_status")

recent_enrollments = (
    df_enrollments
    .filter(F.col("enrolled_at") >= F.lit("2025-01-31 09:00:00").cast("timestamp"))
    .select(
        "id",
        "student_id",
        "course_id",
        "score",
        "grade_status",
        "enrolled_at",
        F.abs(F.col("score") - 57.6).alias("score_distance"),
        F.substring(F.trim(F.col("grade_status")), 1, 3).alias("grade_status_prefix"),
    )
)

sequenced = (
    recent_enrollments
    .select(
        "*",
        F.min("score").over(w_all_by_score_distance_runnin).alias("min_score_so_far"),
        F.count(F.lit(1)).over(w_student_id_grade_status).alias("rows_in_student_id_grade_status"),
        F.min("score_distance").over(w_all_by_score_distance_runnin).alias("min_score_distance_so_far"),
        F.round(F.col("score") - F.avg("score").over(w_student_id_grade_status), 2).alias("score_vs_student_id_grade_status_avg"),
        F.round(F.percent_rank().over(w_all_by_score_distance_ties), 3).alias("score_distance_percent_rank"),
        F.min("score_distance").over(w_student_id_grade_status).alias("student_id_grade_status_min_score_distance"),
        F.min("score").over(w_student_id_grade_status).alias("student_id_grade_status_min_score"),
        F.round(F.cume_dist().over(w_all_by_score_distance), 3).alias("score_distance_cume_dist"),
    )
)

with_windows = (
    sequenced
    .select(
        "*",
        (F.col("score_distance") - F.lag("score_distance").over(w_all_by_student_id_grade_stat)).alias("score_distance_change"),
        (F.col("min_score_distance_so_far") - F.lag("min_score_distance_so_far").over(w_grade_status_by_student_id_g)).alias("min_score_distance_so_far_change"),
        F.first("min_score_so_far").over(w_grade_status_student_id_by_e).alias("first_min_score_so_far"),
        F.round(F.col("score_distance") / F.nullif(F.sum("score_distance").over(w_student_id), F.lit(0)), 4).alias("score_distance_share_of_student_id"),
        F.size(F.collect_set("student_id").over(w_grade_status)).alias("distinct_student_id_in_grade_status"),
    )
)

result = (
    with_windows
    .select(
        "id",
        "student_id",
        "score",
        "grade_status",
        "enrolled_at",
        "score_distance",
        "grade_status_prefix",
        "min_score_so_far",
        "rows_in_student_id_grade_status",
        "min_score_distance_so_far",
        "score_vs_student_id_grade_status_avg",
        "score_distance_percent_rank",
        "student_id_grade_status_min_score_distance",
        "student_id_grade_status_min_score",
        "score_distance_change",
        "min_score_distance_so_far_change",
        "score_distance_share_of_student_id",
        "distinct_student_id_in_grade_status",
        (F.col("score") + F.col("score_distance_change")).alias("score_plus_score_distance_change"),
        F.length(F.trim(F.col("grade_status"))).alias("grade_status_len"),
    )
    .orderBy(
        F.col("grade_status").desc(),
        F.col("student_id_grade_status_min_score").asc_nulls_last(),
    )
)
