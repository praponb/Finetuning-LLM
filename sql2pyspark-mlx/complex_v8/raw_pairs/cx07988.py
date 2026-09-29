from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_grade_status_term_by_enrolle = Window.partitionBy("grade_status", "term").orderBy("enrolled_at", "id")
w_student_id_grade_status = Window.partitionBy("student_id", "grade_status")
w_grade_status_by_enrolled_at = Window.partitionBy("grade_status").orderBy("enrolled_at", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_score_distance_full = Window.orderBy("score_distance", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_grade_status_by_enrolled_at_2 = Window.partitionBy("grade_status").orderBy("enrolled_at", "id")
w_course_id_student_id_by_scor = Window.partitionBy("course_id", "student_id").orderBy(F.col("score_distance").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_score = Window.orderBy("score", "id")
w_term = Window.partitionBy("term")
w_distinct_term_in_student_id = Window.partitionBy("distinct_term_in_student_id_grade_status").orderBy(F.col("second_enrolled_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_distinct_term_in_student_id_2 = Window.partitionBy("distinct_term_in_student_id_grade_status").orderBy(F.col("second_enrolled_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_distinct_term_in_student_id_3 = Window.partitionBy("distinct_term_in_student_id_grade_status").orderBy(F.col("score_distance_share_of_student_id_grade_status").desc_nulls_first(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

prepared = (
    df_enrollments
    .filter((F.col("course_id") >= 11) & (F.col("student_id") <= 7))
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
        "enrolled_at",
        F.abs(F.col("score") - 62.5).alias("score_distance"),
        F.substring(F.trim(F.col("term")), 1, 3).alias("term_prefix"),
    )
)

with_windows = (
    prepared
    .select(
        "id",
        "term",
        "score",
        "score_distance",
        "term_prefix",
        F.round(F.cume_dist().over(w_grade_status_term_by_enrolle), 3).alias("enrolled_at_cume_dist"),
        F.round(F.col("score") / F.nullif(F.sum("score").over(w_student_id_grade_status), F.lit(0)), 4).alias("score_share_of_student_id_grade_status"),
        F.nth_value("enrolled_at", 2).over(w_grade_status_by_enrolled_at).alias("second_enrolled_at"),
        F.size(F.collect_set("term").over(w_student_id_grade_status)).alias("distinct_term_in_student_id_grade_status"),
        F.first("enrolled_at").over(w_all_by_score_distance_full).alias("first_enrolled_at"),
        (F.col("score_distance") - F.lag("score_distance").over(w_grade_status_by_enrolled_at_2)).alias("score_distance_change"),
        F.nth_value("score_distance", 2).over(w_course_id_student_id_by_scor).alias("second_score_distance"),
        F.row_number().over(w_grade_status_by_enrolled_at_2).alias("enrolled_at_seq"),
        F.size(F.collect_set("course_id").over(w_student_id_grade_status)).alias("distinct_course_id_in_student_id_grade_status"),
        F.sum("score").over(w_student_id_grade_status).alias("student_id_grade_status_sum_score"),
        F.round(F.col("score_distance") / F.nullif(F.sum("score_distance").over(w_student_id_grade_status), F.lit(0)), 4).alias("score_distance_share_of_student_id_grade_status"),
        F.round(F.avg("score").over(w_student_id_grade_status), 2).alias("student_id_grade_status_avg_score"),
        F.row_number().over(w_all_by_score).alias("seq_no"),
    )
)

leaders = (
    with_windows
    .filter(F.col("enrolled_at_seq") == 1)
    .drop("enrolled_at_seq")
)

sequenced = (
    leaders
    .select(
        "id",
        "term",
        "score",
        "term_prefix",
        "enrolled_at_cume_dist",
        "score_share_of_student_id_grade_status",
        "second_enrolled_at",
        "distinct_term_in_student_id_grade_status",
        "first_enrolled_at",
        "score_distance_change",
        "second_score_distance",
        "distinct_course_id_in_student_id_grade_status",
        "student_id_grade_status_sum_score",
        "score_distance_share_of_student_id_grade_status",
        "student_id_grade_status_avg_score",
        "seq_no",
        F.count(F.lit(1)).over(w_term).alias("rows_in_term"),
        F.nth_value("second_score_distance", 2).over(w_distinct_term_in_student_id).alias("second_second_score_distance"),
        F.round(F.avg("score").over(w_term), 2).alias("term_avg_score"),
        F.count(F.lit(1)).over(w_distinct_term_in_student_id_2).alias("running_count"),
        F.min("enrolled_at_cume_dist").over(w_distinct_term_in_student_id_3).alias("min_enrolled_at_cume_dist_so_far"),
    )
)

result = (
    sequenced
    .filter(F.col("distinct_course_id_in_student_id_grade_status") > 2)
    .select(
        "id",
        "score",
        "enrolled_at_cume_dist",
        "score_share_of_student_id_grade_status",
        "second_enrolled_at",
        "first_enrolled_at",
        "score_distance_change",
        "second_score_distance",
        "distinct_course_id_in_student_id_grade_status",
        "student_id_grade_status_sum_score",
        "score_distance_share_of_student_id_grade_status",
        "seq_no",
        "term_avg_score",
        "running_count",
        "min_enrolled_at_cume_dist_so_far",
        F.when(F.col("term").isin("2024T1", "2025T1"), "group_a").when(F.col("term") == "2024T2", "group_b").otherwise("other").alias("term_group"),
        F.substring(F.trim(F.col("term_prefix")), 1, 4).alias("term_prefix_prefix"),
    )
)
