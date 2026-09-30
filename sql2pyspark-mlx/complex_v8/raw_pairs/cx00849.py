from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_course_id = Window.partitionBy("course_id")
w_grade_status_course_id = Window.partitionBy("grade_status", "course_id")
w_all_by_score_ties = Window.orderBy(F.col("score").desc())
w_course_id_enrolled_at_month = Window.partitionBy("course_id", F.month("enrolled_at")).orderBy(F.col("score_filled").asc_nulls_last())
w_all_by_score = Window.orderBy(F.col("score").desc(), "id")
w_all_by_enrolled_at_full = Window.orderBy(F.col("enrolled_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_enrolled_at_full_2 = Window.orderBy("enrolled_at", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_distinct_student_id_in_grade = Window.partitionBy("distinct_student_id_in_grade_status_course_id", F.date_trunc("month", "enrolled_at"))

prepared_enrollments = (
    df_enrollments
    .filter(F.col("term") == "2025T1")
    .select(
        "id",
        "student_id",
        "course_id",
        "score",
        "grade_status",
        "enrolled_at",
        F.coalesce(F.col("score"), F.lit(0.0)).alias("score_filled"),
    )
)

windowed = (
    prepared_enrollments
    .select(
        "id",
        "student_id",
        "course_id",
        "enrolled_at",
        "score_filled",
        F.round(F.col("score_filled") / F.nullif(F.sum("score_filled").over(w_course_id), F.lit(0)), 4).alias("score_filled_share_of_course_id"),
        F.size(F.collect_set("student_id").over(w_grade_status_course_id)).alias("distinct_student_id_in_grade_status_course_id"),
        F.round(F.cume_dist().over(w_all_by_score_ties), 3).alias("score_cume_dist"),
        F.round(F.percent_rank().over(w_course_id_enrolled_at_month), 3).alias("score_filled_percent_rank"),
        F.max("score_filled").over(w_grade_status_course_id).alias("grade_status_course_id_max_score_filled"),
        (F.col("score") - F.lag("score").over(w_all_by_score)).alias("score_change"),
        F.nth_value("score_filled", 2).over(w_all_by_enrolled_at_full).alias("second_score_filled"),
        F.sum("score_filled").over(w_grade_status_course_id).alias("grade_status_course_id_sum_score_filled"),
    )
)

metrics = (
    windowed
    .select(
        "id",
        "student_id",
        "course_id",
        "enrolled_at",
        "score_filled",
        "distinct_student_id_in_grade_status_course_id",
        "score_cume_dist",
        "grade_status_course_id_max_score_filled",
        "grade_status_course_id_sum_score_filled",
        F.size(F.collect_set("student_id").over(w_course_id)).alias("distinct_student_id_in_course_id"),
        F.last("score_cume_dist").over(w_all_by_enrolled_at_full_2).alias("last_score_cume_dist"),
        F.size(F.collect_set("course_id").over(w_distinct_student_id_in_grade)).alias("distinct_course_id_in_distinct_student_id_in_gra"),
    )
)

result = (
    metrics
    .select(
        "id",
        "student_id",
        "course_id",
        "distinct_student_id_in_grade_status_course_id",
        "score_cume_dist",
        "grade_status_course_id_max_score_filled",
        "distinct_student_id_in_course_id",
        "distinct_course_id_in_distinct_student_id_in_gra",
        F.coalesce(F.col("score_filled"), F.lit(0.0)).alias("score_filled_filled"),
        F.coalesce(F.col("score_filled"), F.lit(0.0)).alias("score_filled_filled_2"),
    )
    .orderBy(
        F.col("distinct_student_id_in_course_id").desc(),
        F.col("course_id").desc(),
    )
)
