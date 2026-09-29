from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_enrolled_at = Window.orderBy(F.col("enrolled_at").asc_nulls_last(), "id")
w_all_by_score = Window.orderBy(F.col("score").desc(), "id")
w_enrolled_at_month_student_id = Window.partitionBy(F.date_trunc("month", "enrolled_at"), "student_id")
w_all_by_score_full = Window.orderBy("score", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_score_ties = Window.orderBy(F.col("score").desc())
w_all_by_score_2 = Window.orderBy("score", "id")

valid_enrollments = (
    df_enrollments
    .filter((F.col("course_id").isNotNull()) & (F.col("term").isin("2025T1", "2025T2")))
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

ranked = (
    valid_enrollments
    .select(
        "*",
        F.lead("score").over(w_all_by_enrolled_at).alias("next_score"),
        F.row_number().over(w_all_by_score).alias("seq_no"),
        F.round(F.avg("score").over(w_enrolled_at_month_student_id), 2).alias("enrolled_at_month_student_id_avg_score"),
        F.round(F.col("score") / F.nullif(F.sum("score").over(w_enrolled_at_month_student_id), F.lit(0)), 4).alias("score_share_of_enrolled_at_month_student_id"),
        F.nth_value("enrolled_at", 2).over(w_all_by_score_full).alias("second_enrolled_at"),
        F.rank().over(w_all_by_score_ties).alias("score_rank"),
        F.size(F.collect_set("course_id").over(w_enrolled_at_month_student_id)).alias("distinct_course_id_in_enrolled_at_month_student_"),
        F.round(F.cume_dist().over(w_all_by_score_ties), 3).alias("score_cume_dist"),
        (F.col("score") - F.lag("score").over(w_all_by_score_2)).alias("score_change"),
        F.round(F.col("score") - F.avg("score").over(w_enrolled_at_month_student_id), 2).alias("score_vs_enrolled_at_month_student_id_avg"),
        F.min("score").over(w_enrolled_at_month_student_id).alias("enrolled_at_month_student_id_min_score"),
    )
)

top_rows = ranked.filter(F.col("seq_no") <= 5)

result = (
    top_rows
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "grade_status",
        "enrolled_at",
        "seq_no",
        "enrolled_at_month_student_id_avg_score",
        "score_share_of_enrolled_at_month_student_id",
        "score_rank",
        "distinct_course_id_in_enrolled_at_month_student_",
        "score_cume_dist",
        "score_change",
        "score_vs_enrolled_at_month_student_id_avg",
    )
    .orderBy("id")
)
