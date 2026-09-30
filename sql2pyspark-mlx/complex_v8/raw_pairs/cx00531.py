from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_grade_status = Window.partitionBy("grade_status")
w_all_by_enrolled_at_full = Window.orderBy("enrolled_at", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_enrolled_at = Window.orderBy("enrolled_at", "id")
w_student_id_by_score_prior_1 = Window.partitionBy("student_id").orderBy("score", "id").rowsBetween(-1, -1)
w_student_id_by_score_running = Window.partitionBy("student_id").orderBy("score", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_student_id_by_score_moving_3 = Window.partitionBy("student_id").orderBy("score", "id").rowsBetween(-3, Window.currentRow)
w_all_by_enrolled_at_ties = Window.orderBy("enrolled_at")
w_student_id_by_score = Window.partitionBy("student_id").orderBy("score", "id")
w_all_by_enrolled_at_running = Window.orderBy("enrolled_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_score_quartile_course_id = Window.partitionBy("score_quartile", "course_id")
w_all_by_running_score_2 = Window.orderBy(F.col("running_score_2").desc(), "id")
w_all_by_last_known_score_prio = Window.orderBy("last_known_score", "id").rowsBetween(-1, -1)
w_all_by_last_known_score_runn = Window.orderBy("last_known_score", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

filtered = (
    df_enrollments
    .filter((F.col("score") < 83.1) & (F.col("enrolled_at") < F.lit("2025-02-12 09:00:00").cast("timestamp")))
    .select(
        "id",
        "student_id",
        "course_id",
        "score",
        "grade_status",
        "enrolled_at",
        F.date_format(F.col("enrolled_at"), "yyyy-MM").alias("enrolled_at_label"),
        F.lower(F.trim(F.col("grade_status"))).alias("grade_status_clean"),
    )
)

with_trends = (
    filtered
    .select(
        "*",
        F.count(F.lit(1)).over(w_grade_status).alias("rows_in_grade_status"),
        F.nth_value("score", 2).over(w_all_by_enrolled_at_full).alias("second_score"),
        F.lag("enrolled_at").over(w_all_by_enrolled_at).alias("prev_enrolled_at"),
        F.sum("score").over(w_student_id_by_score_prior_1).alias("score_sum_1_prior"),
        F.sum("score").over(w_student_id_by_score_running).alias("running_score"),
        F.sum("score").over(w_student_id_by_score_moving_3).alias("score_sum_4"),
        F.max("score").over(w_grade_status).alias("grade_status_max_score"),
        F.round(F.cume_dist().over(w_all_by_enrolled_at_ties), 3).alias("enrolled_at_cume_dist"),
        F.ntile(4).over(w_student_id_by_score).alias("score_quartile"),
        F.round(F.col("score") / F.nullif(F.sum("score").over(w_grade_status), F.lit(0)), 4).alias("score_share_of_grade_status"),
        F.lead("score", 2).over(w_student_id_by_score).alias("next2_score"),
        F.ntile(10).over(w_all_by_enrolled_at).alias("enrolled_at_decile"),
        F.last("score", ignorenulls=True).over(w_all_by_enrolled_at_running).alias("last_known_score"),
        F.sum("score").over(w_all_by_enrolled_at_running).alias("running_score_2"),
    )
)

with_trends_2 = (
    with_trends
    .select(
        "id",
        "student_id",
        "course_id",
        "score",
        "grade_status",
        "score_sum_1_prior",
        "running_score",
        "score_sum_4",
        "grade_status_max_score",
        "score_quartile",
        "score_share_of_grade_status",
        "next2_score",
        "enrolled_at_decile",
        "last_known_score",
        "running_score_2",
        F.round(F.col("enrolled_at_decile") / F.nullif(F.sum("enrolled_at_decile").over(w_score_quartile_course_id), F.lit(0)), 4).alias("enrolled_at_decile_share_of_score_quartile_cours"),
        F.round(F.col("score_sum_1_prior") / F.nullif(F.sum("score_sum_1_prior").over(w_score_quartile_course_id), F.lit(0)), 4).alias("score_sum_1_prior_share_of_score_quartile_course"),
        F.ntile(3).over(w_all_by_running_score_2).alias("running_score_2_tercile"),
        F.max("running_score_2").over(w_all_by_last_known_score_prio).alias("running_score_2_max_1_prior"),
        F.max("running_score").over(w_score_quartile_course_id).alias("score_quartile_course_id_max_running_score"),
        F.min("enrolled_at_cume_dist").over(w_all_by_last_known_score_runn).alias("min_enrolled_at_cume_dist_so_far"),
        F.size(F.collect_set("student_id").over(w_score_quartile_course_id)).alias("distinct_student_id_in_score_quartile_course_id"),
        F.round(F.col("running_score") / F.nullif(F.sum("running_score").over(w_score_quartile_course_id), F.lit(0)), 4).alias("running_score_share_of_score_quartile_course_id"),
    )
)

result = (
    with_trends_2
    .filter(F.col("running_score") < 900.0)
    .select(
        "id",
        "student_id",
        "course_id",
        "score",
        "grade_status",
        "running_score",
        "score_sum_4",
        "grade_status_max_score",
        "score_quartile",
        "score_share_of_grade_status",
        "next2_score",
        "enrolled_at_decile",
        "last_known_score",
        "running_score_2",
        "running_score_2_tercile",
        "score_quartile_course_id_max_running_score",
        "min_enrolled_at_cume_dist_so_far",
        "distinct_student_id_in_score_quartile_course_id",
        "running_score_share_of_score_quartile_course_id",
    )
    .orderBy(
        F.col("last_known_score").asc_nulls_last(),
        F.col("score_share_of_grade_status").asc(),
    )
)
