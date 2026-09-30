from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_enrolled_at = Window.orderBy("enrolled_at", "id")
w_grade_status_or_unknown_grad = Window.partitionBy("grade_status_or_unknown", "grade_status").orderBy(F.col("score").desc(), "id").rowsBetween(-3, -1)
w_course_credits_by_enrolled_a = Window.partitionBy("course_credits").orderBy("enrolled_at", "id")
w_term_by_course_credits_ties = Window.partitionBy("term").orderBy("course_credits")
w_all_by_enrolled_at_ties = Window.orderBy("enrolled_at")
w_course_id = Window.partitionBy("course_id")
w_all_by_score = Window.orderBy("score", "id")
w_course_credits_by_enrolled_a_2 = Window.partitionBy("course_credits").orderBy("enrolled_at")
w_course_credits_course_level = Window.partitionBy("course_credits", "course_level").orderBy(F.col("course_credits").desc())
w_course_credits_course_level_2 = Window.partitionBy("course_credits", "course_level").orderBy(F.col("course_credits").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_next2_course_credits_by_cour = Window.partitionBy("next2_course_credits").orderBy("course_credits_rank")
w_student_id_seq_no = Window.partitionBy("student_id", "seq_no")
w_all_by_next2_course_credits = Window.orderBy(F.col("next2_course_credits").desc(), "id")
w_all_by_rn_moving_2 = Window.orderBy(F.col("rn").desc(), "id").rowsBetween(-2, Window.currentRow)
w_course_credits_dense_rank_gr = Window.partitionBy("course_credits_dense_rank", "grade_status")

recent = (
    df_enrollments
    .filter(F.col("enrolled_at") >= F.lit("2025-02-06 10:00:00").cast("timestamp"))
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
        "enrolled_at",
        F.coalesce(F.col("grade_status"), F.lit("UNKNOWN")).alias("grade_status_or_unknown"),
    )
)

with_parent_course = (
    recent.alias("s")
    .join(df_courses.alias("c"), F.col("c.id") == F.col("s.course_id"), "left")
    .select(
        "s.*",
        F.col("c.credits").alias("course_credits"),
        F.col("c.level").alias("course_level"),
    )
)

windowed = (
    with_parent_course
    .select(
        "id",
        "student_id",
        "course_id",
        "grade_status",
        "enrolled_at",
        "grade_status_or_unknown",
        "course_credits",
        F.lead("course_credits", 1, 0).over(w_all_by_enrolled_at).alias("next_course_credits"),
        F.max("score").over(w_grade_status_or_unknown_grad).alias("score_max_3_prior"),
        F.lag("course_credits").over(w_course_credits_by_enrolled_a).alias("prev_course_credits"),
        F.rank().over(w_term_by_course_credits_ties).alias("course_credits_rank"),
        F.rank().over(w_all_by_enrolled_at_ties).alias("enrolled_at_rank"),
        F.row_number().over(w_course_credits_by_enrolled_a).alias("seq_no"),
        F.row_number().over(w_all_by_enrolled_at).alias("enrolled_at_seq"),
        F.round(F.avg("course_credits").over(w_course_id), 2).alias("course_id_avg_course_credits"),
        F.lag("course_credits", 2).over(w_all_by_score).alias("prev2_course_credits"),
        F.round(F.cume_dist().over(w_course_credits_by_enrolled_a_2), 3).alias("enrolled_at_cume_dist"),
        F.dense_rank().over(w_course_credits_course_level).alias("course_credits_dense_rank"),
        F.lead("course_credits", 2, 0).over(w_all_by_enrolled_at).alias("next2_course_credits"),
        F.first("enrolled_at", ignorenulls=True).over(w_course_credits_course_level_2).alias("first_known_enrolled_at"),
        F.row_number().over(w_all_by_score).alias("rn"),
    )
)

first_rows = windowed.filter(F.col("enrolled_at_seq") <= 5)

sequenced = (
    first_rows
    .select(
        "*",
        F.dense_rank().over(w_next2_course_credits_by_cour).alias("course_credits_rank_dense_rank"),
        F.count(F.lit(1)).over(w_student_id_seq_no).alias("rows_in_student_id_seq_no"),
        (F.col("enrolled_at_seq") - F.lag("enrolled_at_seq").over(w_all_by_next2_course_credits)).alias("enrolled_at_seq_change"),
        F.max("enrolled_at_cume_dist").over(w_all_by_rn_moving_2).alias("enrolled_at_cume_dist_max_3"),
        F.min("course_id_avg_course_credits").over(w_student_id_seq_no).alias("student_id_seq_no_min_course_id_avg_course_credi"),
        F.max("enrolled_at_cume_dist").over(w_course_credits_dense_rank_gr).alias("course_credits_dense_rank_grade_status_max_enrol"),
    )
)

per_group = (
    sequenced
    .groupBy(
        "grade_status",
        "course_credits_rank",
        F.date_trunc("month", "first_known_enrolled_at").alias("first_known_enrolled_at_month"),
    )
    .agg(
        F.array_sort(F.collect_set("grade_status")).alias("grade_status_list"),
        F.sum(F.when(F.col("grade_status").isin("PASS", "FAIL", "INCOMPLETE"), F.col("course_credits_dense_rank"))).alias("course_credits_dense_rank_grade_status_in_pass_f"),
        F.sum("course_credits_rank_dense_rank").alias("total_course_credits_rank_dense_rank"),
        F.count("*").alias("n_rows"),
    )
)

result = (
    per_group
    .select(
        "grade_status",
        "course_credits_rank",
        "first_known_enrolled_at_month",
        "grade_status_list",
        "course_credits_dense_rank_grade_status_in_pass_f",
        "total_course_credits_rank_dense_rank",
        F.coalesce(F.col("course_credits_rank"), F.lit(0)).alias("course_credits_rank_filled"),
        F.coalesce(F.col("n_rows"), F.lit(0)).alias("n_rows_filled"),
    )
    .orderBy("course_credits_rank_filled", F.col("n_rows_filled").desc())
)
