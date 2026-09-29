from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_grade_status_term_by_enrolle = Window.partitionBy("grade_status", "term").orderBy("enrolled_at", "id")
w_student_id_term_by_score = Window.partitionBy("student_id", "term").orderBy(F.col("score").desc_nulls_first(), "id")
w_grade_status_term_by_enrolle_2 = Window.partitionBy("grade_status", "term").orderBy("enrolled_at")
w_student_id = Window.partitionBy("student_id")
w_all_by_score_ties = Window.orderBy("score")
w_all_by_enrolled_at = Window.orderBy(F.col("enrolled_at").desc(), "id")
w_student_id_grade_status = Window.partitionBy("student_id", "grade_status")

recent_enrollments = (
    df_enrollments
    .filter(F.hour(F.col("enrolled_at")).between(10, 14))
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
        "enrolled_at",
        F.substring(F.trim(F.col("grade_status")), 1, 4).alias("grade_status_prefix"),
        F.length(F.trim(F.col("grade_status"))).alias("grade_status_len"),
    )
)

with_trends = (
    recent_enrollments
    .select(
        "id",
        "student_id",
        "term",
        "score",
        "grade_status",
        "enrolled_at",
        "grade_status_prefix",
        F.round((F.unix_timestamp("enrolled_at") - F.unix_timestamp(F.lag("enrolled_at").over(w_grade_status_term_by_enrolle))) / 60, 1).alias("minutes_since_prev_enrolled_at"),
        F.lag("grade_status_len", 2).over(w_student_id_term_by_score).alias("prev2_grade_status_len"),
        F.round(F.cume_dist().over(w_grade_status_term_by_enrolle_2), 3).alias("enrolled_at_cume_dist"),
        F.round(F.col("grade_status_len") / F.nullif(F.sum("grade_status_len").over(w_student_id), F.lit(0)), 4).alias("grade_status_len_share_of_student_id"),
        F.lag("score", 2).over(w_grade_status_term_by_enrolle).alias("prev2_score"),
        F.lead("enrolled_at").over(w_grade_status_term_by_enrolle).alias("next_enrolled_at"),
    )
)

without_matches = (
    with_trends.alias("s")
    .join(df_enrollments.alias("e"), (F.col("e.student_id") == F.col("s.student_id")) & (~F.col("e.grade_status").isin("INCOMPLETE", "WITHDRAWN")), "left_semi")
)

enrollment_stats = (
    df_enrollments
    .groupBy("student_id")
    .agg(
        F.min("enrolled_at").alias("first_enrollment_enrolled_at"),
        F.max("enrolled_at").alias("last_enrollment_enrolled_at"),
        F.max("enrolled_at").alias("last_enrollment_enrolled_at_2"),
    )
)

with_counts = (
    without_matches.alias("s")
    .join(enrollment_stats.alias("e"), F.col("e.student_id") == F.col("s.student_id"), "left")
    .select(
        "s.*",
        "e.first_enrollment_enrolled_at",
        "e.last_enrollment_enrolled_at",
        "e.last_enrollment_enrolled_at_2",
    )
)

sequenced = (
    with_counts
    .select(
        "*",
        F.round(F.percent_rank().over(w_all_by_score_ties), 3).alias("score_percent_rank"),
        F.round(F.percent_rank().over(w_all_by_enrolled_at), 3).alias("enrolled_at_percent_rank"),
        F.count(F.lit(1)).over(w_student_id_grade_status).alias("rows_in_student_id_grade_status"),
        F.round(F.cume_dist().over(w_all_by_enrolled_at), 3).alias("enrolled_at_cume_dist_2"),
        F.dense_rank().over(w_all_by_score_ties).alias("score_dense_rank"),
    )
)

result = (
    sequenced
    .filter(F.col("enrolled_at") < F.lit("2025-02-24 13:00:00").cast("timestamp"))
    .select(
        "id",
        "student_id",
        "term",
        "score",
        "grade_status",
        "enrolled_at",
        "grade_status_prefix",
        "minutes_since_prev_enrolled_at",
        "prev2_grade_status_len",
        "grade_status_len_share_of_student_id",
        "prev2_score",
        "first_enrollment_enrolled_at",
        "last_enrollment_enrolled_at",
        "last_enrollment_enrolled_at_2",
        "score_percent_rank",
        "enrolled_at_percent_rank",
        "rows_in_student_id_grade_status",
        "enrolled_at_cume_dist_2",
        "score_dense_rank",
        F.abs(F.col("score") - 42.4).alias("score_distance"),
    )
)
