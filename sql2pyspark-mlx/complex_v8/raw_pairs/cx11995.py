from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_student_id_course_id = Window.partitionBy("student_id", "course_id")
w_all_by_score = Window.orderBy("score", "id")
w_student_id_by_enrolled_at = Window.partitionBy("student_id").orderBy("enrolled_at", "id")
w_enrolled_at_month_term = Window.partitionBy(F.month("enrolled_at"), "term")
w_all_by_score_bucket = Window.orderBy("score_bucket", "id")

filtered_enrollments = (
    df_enrollments
    .filter((F.col("term") == "2024T2") & (F.hour(F.col("enrolled_at")).between(10, 16)))
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
        "enrolled_at",
        (F.floor(F.col("score") / 25) * 25).alias("score_bucket"),
        F.lower(F.regexp_replace(F.trim(F.col("term")), " ", "_")).alias("term_slug"),
    )
)

with_windows = (
    filtered_enrollments
    .select(
        "*",
        F.min("score_bucket").over(w_student_id_course_id).alias("student_id_course_id_min_score_bucket"),
        F.row_number().over(w_all_by_score).alias("seq_no"),
        F.round((F.unix_timestamp("enrolled_at") - F.unix_timestamp(F.lag("enrolled_at").over(w_student_id_by_enrolled_at))) / 60, 1).alias("minutes_since_prev_enrolled_at"),
        F.min("score_bucket").over(w_enrolled_at_month_term).alias("enrolled_at_month_term_min_score_bucket"),
        F.round(F.cume_dist().over(w_all_by_score_bucket), 3).alias("score_bucket_cume_dist"),
        F.round(F.col("score") / F.nullif(F.sum("score").over(w_student_id_course_id), F.lit(0)), 4).alias("score_share_of_student_id_course_id"),
        F.round(F.col("score_bucket") / F.nullif(F.sum("score_bucket").over(w_enrolled_at_month_term), F.lit(0)), 4).alias("score_bucket_share_of_enrolled_at_month_term"),
        F.lag("score", 2).over(w_student_id_by_enrolled_at).alias("prev2_score"),
        F.size(F.collect_set("grade_status").over(w_student_id_course_id)).alias("distinct_grade_status_in_student_id_course_id"),
        F.lag("score", 3).over(w_student_id_by_enrolled_at).alias("prev3_score"),
    )
)

leaders = with_windows.filter(F.col("seq_no") == 1)

stats = (
    leaders
    .groupBy("grade_status", "term", "distinct_grade_status_in_student_id_course_id")
    .agg(
        F.round(F.sum("score_share_of_student_id_course_id"), 2).alias("total_score_share_of_student_id_course_id"),
        F.sum("score_bucket").alias("total_score_bucket"),
        F.max("score_share_of_student_id_course_id").alias("max_score_share_of_student_id_course_id_check"),
    )
    .filter(F.col("max_score_share_of_student_id_course_id_check") > 0.3)
    .drop("max_score_share_of_student_id_course_id_check")
)

result = (
    stats
    .select(
        "grade_status",
        "term",
        "distinct_grade_status_in_student_id_course_id",
        "total_score_share_of_student_id_course_id",
        F.substring(F.trim(F.col("term")), 1, 2).alias("term_prefix"),
    )
    .orderBy(
        F.col("term_prefix").asc_nulls_last(),
        "grade_status",
        "term",
        "distinct_grade_status_in_student_id_course_id",
    )
    .limit(5)
)
