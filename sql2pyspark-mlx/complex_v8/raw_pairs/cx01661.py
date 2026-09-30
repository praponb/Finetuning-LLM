from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_student_id_by_enrolled_at = Window.partitionBy("student_id").orderBy(F.col("enrolled_at").desc(), "id")
w_term_grade_status = Window.partitionBy("term", "grade_status")
w_student_id_course_id = Window.partitionBy("student_id", "course_id")
w_term_by_enrolled_at_full = Window.partitionBy("term").orderBy("enrolled_at", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_score = Window.orderBy(F.col("score").desc(), "id")
w_all_by_score_prior_3 = Window.orderBy(F.col("score").desc(), "id").rowsBetween(-3, -1)
w_all_by_score_2 = Window.orderBy("score", "id")
w_term_by_enrolled_at = Window.partitionBy("term").orderBy("enrolled_at", "id")

cleaned_enrollments = (
    df_enrollments
    .filter((F.col("enrolled_at") >= F.lit("2025-01-31 09:00:00").cast("timestamp")) & (F.col("score") <= 87.8))
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

sequenced = (
    cleaned_enrollments
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "enrolled_at",
        F.ntile(3).over(w_student_id_by_enrolled_at).alias("enrolled_at_tercile"),
        F.size(F.collect_set("course_id").over(w_term_grade_status)).alias("distinct_course_id_in_term_grade_status"),
        F.min("score").over(w_student_id_course_id).alias("student_id_course_id_min_score"),
        F.first("score").over(w_term_by_enrolled_at_full).alias("first_score"),
        F.row_number().over(w_all_by_score).alias("rn"),
        F.round(F.col("score") / F.nullif(F.sum("score").over(w_term_grade_status), F.lit(0)), 4).alias("score_share_of_term_grade_status"),
        F.nth_value("score", 2).over(w_term_by_enrolled_at_full).alias("second_score"),
        F.round(F.avg("score").over(w_all_by_score_prior_3), 2).alias("score_avg_3_prior"),
        F.sum("score").over(w_student_id_course_id).alias("student_id_course_id_sum_score"),
        F.ntile(4).over(w_all_by_score_2).alias("score_quartile"),
        F.dense_rank().over(w_term_by_enrolled_at).alias("enrolled_at_dense_rank"),
    )
)

leaders = (
    sequenced
    .filter(F.col("rn") <= 5)
    .drop("rn")
)

result = (
    leaders
    .filter(F.col("first_score") <= 79.2)
    .select(
        "id",
        "term",
        "score",
        "enrolled_at",
        "enrolled_at_tercile",
        "distinct_course_id_in_term_grade_status",
        "student_id_course_id_min_score",
        "first_score",
        "score_share_of_term_grade_status",
        "second_score",
        "score_quartile",
        "enrolled_at_dense_rank",
        (F.floor(F.col("enrolled_at_dense_rank") / 2) * 2).alias("enrolled_at_dense_rank_bucket"),
        F.greatest(F.col("first_score"), F.col("score")).alias("greatest_first_score_score"),
    )
    .orderBy("first_score", F.col("score_share_of_term_grade_status").desc())
)
