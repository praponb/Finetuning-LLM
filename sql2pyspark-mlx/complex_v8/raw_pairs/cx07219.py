from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_grade_status_by_enrolled_at = Window.partitionBy("grade_status").orderBy("enrolled_at_plus_1d", "id").rowsBetween(-3, -1)
w_grade_status_by_enrolled_at_2 = Window.partitionBy("grade_status").orderBy("enrolled_at_plus_1d")
w_all_by_enrolled_at = Window.orderBy("enrolled_at", "id")
w_course_id = Window.partitionBy("course_id")
w_course_id_student_id_by_enro = Window.partitionBy("course_id", "student_id").orderBy(F.col("enrolled_at").desc(), "id")
w_grade_status = Window.partitionBy("grade_status")
w_all_by_enrolled_at_full = Window.orderBy("enrolled_at", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_enrolled_at_plus_1d_month_by = Window.partitionBy(F.date_trunc("month", "enrolled_at_plus_1d")).orderBy("score", "id")
w_grade_status_by_enrolled_at_3 = Window.partitionBy("grade_status").orderBy("enrolled_at_plus_1d", "id").rowsBetween(-3, Window.currentRow)
w_enrolled_at_plus_1d_to_date = Window.partitionBy(F.to_date("enrolled_at_plus_1d")).orderBy("enrolled_at", "id")
w_enrolled_at_plus_1d_to_date_2 = Window.partitionBy(F.to_date("enrolled_at_plus_1d")).orderBy("enrolled_at")
w_enrolled_at_plus_1d_to_date_3 = Window.partitionBy(F.to_date("enrolled_at_plus_1d")).orderBy("enrolled_at", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_enrolled_at_running = Window.orderBy("enrolled_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_enrolled_at_rank_student_id = Window.partitionBy("enrolled_at_rank", "student_id")
w_all_by_first_enrolled_at_tie = Window.orderBy("first_enrolled_at")
w_score_dense_rank_enrolled_at = Window.partitionBy("score_dense_rank", F.month("enrolled_at"))
w_seq_no_enrolled_at_plus_1d_m = Window.partitionBy("seq_no", F.month("enrolled_at_plus_1d")).orderBy(F.col("enrolled_at_plus_1d").desc_nulls_first(), "id")
w_all_by_first_enrolled_at_ful = Window.orderBy("first_enrolled_at", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_score_share_of_course = Window.orderBy(F.col("score_share_of_course_id").asc_nulls_last())

filtered_enrollments = (
    df_enrollments
    .filter((F.col("grade_status").isin("INCOMPLETE", "PASS")) & (F.to_date(F.col("enrolled_at")) < F.lit("2025-02-18").cast("date")))
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
        "enrolled_at",
        (F.col("enrolled_at") + F.expr("INTERVAL 1 DAYS")).alias("enrolled_at_plus_1d"),
    )
)

ranked = (
    filtered_enrollments
    .select(
        "*",
        F.round(F.avg("score").over(w_grade_status_by_enrolled_at), 2).alias("score_avg_3_prior"),
        F.rank().over(w_grade_status_by_enrolled_at_2).alias("enrolled_at_plus_1d_rank"),
        F.row_number().over(w_all_by_enrolled_at).alias("seq_no"),
        F.count(F.lit(1)).over(w_course_id).alias("rows_in_course_id"),
        F.lead("enrolled_at_plus_1d").over(w_course_id_student_id_by_enro).alias("next_enrolled_at_plus_1d"),
        F.sum("score").over(w_grade_status).alias("grade_status_sum_score"),
        F.round(F.col("score") / F.nullif(F.sum("score").over(w_course_id), F.lit(0)), 4).alias("score_share_of_course_id"),
        F.first("enrolled_at").over(w_all_by_enrolled_at_full).alias("first_enrolled_at"),
        F.dense_rank().over(w_enrolled_at_plus_1d_month_by).alias("score_dense_rank"),
        F.max("score").over(w_grade_status_by_enrolled_at_3).alias("score_max_4"),
        F.lag("enrolled_at_plus_1d", 2).over(w_enrolled_at_plus_1d_to_date).alias("prev2_enrolled_at_plus_1d"),
        F.rank().over(w_enrolled_at_plus_1d_to_date_2).alias("enrolled_at_rank"),
        F.round(F.col("score") / F.nullif(F.sum("score").over(w_grade_status), F.lit(0)), 4).alias("score_share_of_grade_status"),
        F.nth_value("enrolled_at_plus_1d", 2).over(w_enrolled_at_plus_1d_to_date_3).alias("second_enrolled_at_plus_1d"),
        F.first("score", ignorenulls=True).over(w_all_by_enrolled_at_running).alias("first_known_score"),
    )
)

top_rows = ranked.filter(F.col("score_dense_rank") == 1)

sequenced = (
    top_rows
    .select(
        "*",
        F.size(F.collect_set("course_id").over(w_enrolled_at_rank_student_id)).alias("distinct_course_id_in_enrolled_at_rank_student_i"),
        F.round(F.cume_dist().over(w_all_by_first_enrolled_at_tie), 3).alias("first_enrolled_at_cume_dist"),
        F.max("score_avg_3_prior").over(w_score_dense_rank_enrolled_at).alias("score_dense_rank_enrolled_at_month_max_score_avg"),
        F.lead("first_enrolled_at").over(w_seq_no_enrolled_at_plus_1d_m).alias("next_first_enrolled_at"),
        F.last("score_max_4").over(w_all_by_first_enrolled_at_ful).alias("last_score_max_4"),
        F.rank().over(w_all_by_score_share_of_course).alias("score_share_of_course_id_rank"),
    )
)

result = (
    sequenced
    .filter(F.col("last_score_max_4") < 255.0)
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
        "enrolled_at",
        "enrolled_at_plus_1d",
        "score_avg_3_prior",
        "enrolled_at_plus_1d_rank",
        "seq_no",
        "rows_in_course_id",
        "next_enrolled_at_plus_1d",
        "grade_status_sum_score",
        "first_enrolled_at",
        "score_dense_rank",
        "score_max_4",
        "prev2_enrolled_at_plus_1d",
        "score_share_of_grade_status",
        "first_known_score",
        "distinct_course_id_in_enrolled_at_rank_student_i",
        "first_enrolled_at_cume_dist",
        "score_dense_rank_enrolled_at_month_max_score_avg",
        "next_first_enrolled_at",
        "score_share_of_course_id_rank",
        F.lower(F.regexp_replace(F.trim(F.col("term")), " ", "_")).alias("term_slug"),
    )
    .orderBy(F.col("score_avg_3_prior").asc_nulls_last(), F.col("term").desc())
)
