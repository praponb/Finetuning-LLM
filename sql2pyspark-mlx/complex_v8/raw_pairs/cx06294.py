from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_score_bucket_prior_2 = Window.orderBy(F.col("score_bucket").asc_nulls_last(), "id").rowsBetween(-2, -1)
w_student_id_by_enrolled_at_mo = Window.partitionBy("student_id").orderBy(F.col("enrolled_at").desc(), "id").rowsBetween(-6, Window.currentRow)
w_term = Window.partitionBy("term")
w_course_id_by_score_distance = Window.partitionBy("course_id").orderBy(F.col("score_distance").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_course_id_by_score_distance_2 = Window.partitionBy("course_id").orderBy(F.col("score_distance").desc(), "id")
w_grade_status = Window.partitionBy("grade_status")
w_course_id_by_score_distance_3 = Window.partitionBy("course_id").orderBy(F.col("score_distance").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

valid_enrollments = (
    df_enrollments
    .filter((F.col("student_id") <= 10) & (F.hour(F.col("enrolled_at")).between(7, 13)))
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "grade_status",
        "enrolled_at",
        (F.floor(F.col("score") / 25) * 25).alias("score_bucket"),
        F.abs(F.col("score") - 57.8).alias("score_distance"),
    )
)

sequenced = (
    valid_enrollments
    .select(
        "*",
        F.max("score_distance").over(w_all_by_score_bucket_prior_2).alias("score_distance_max_2_prior"),
        F.sum("score_bucket").over(w_student_id_by_enrolled_at_mo).alias("score_bucket_sum_7"),
        F.max("score_distance").over(w_term).alias("term_max_score_distance"),
        F.min("score_distance").over(w_course_id_by_score_distance).alias("min_score_distance_so_far"),
        F.lag("score_distance", 3).over(w_course_id_by_score_distance_2).alias("prev3_score_distance"),
        F.size(F.collect_set("student_id").over(w_grade_status)).alias("distinct_student_id_in_grade_status"),
        F.size(F.collect_set("term").over(w_grade_status)).alias("distinct_term_in_grade_status"),
        F.first("score_bucket").over(w_course_id_by_score_distance_3).alias("first_score_bucket"),
    )
)

result = (
    sequenced
    .select(
        "id",
        "term",
        "enrolled_at",
        "score_bucket",
        "score_bucket_sum_7",
        "term_max_score_distance",
        "min_score_distance_so_far",
        "prev3_score_distance",
        "distinct_student_id_in_grade_status",
        "first_score_bucket",
    )
    .orderBy(F.col("term").desc_nulls_first())
)
