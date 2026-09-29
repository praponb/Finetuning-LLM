from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_score_full = Window.orderBy("score", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_student_id_by_score = Window.partitionBy("student_id").orderBy(F.col("score").desc(), "id")
w_all_by_enrolled_at_ties = Window.orderBy(F.col("enrolled_at").asc_nulls_last())
w_enrolled_at_to_date_student = Window.partitionBy(F.to_date("enrolled_at"), "student_id").orderBy("score", "id")
w_student_id_by_score_full = Window.partitionBy("student_id").orderBy(F.col("score").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_student_id_course_id_by_enro = Window.partitionBy("student_id", "course_id").orderBy("enrolled_at", "id").rowsBetween(-2, -1)
w_enrolled_at_year_term = Window.partitionBy(F.year("enrolled_at"), "term")
w_student_id = Window.partitionBy("student_id")
w_enrolled_at_to_date_student_2 = Window.partitionBy(F.to_date("enrolled_at"), "student_id").orderBy("score", "id").rowsBetween(-1, -1)
w_enrolled_at_to_date_student_3 = Window.partitionBy(F.to_date("enrolled_at"), "student_id").orderBy("score", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_seq_no_by_second_enrolled_at = Window.partitionBy("seq_no").orderBy(F.col("second_enrolled_at_2").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_seq_no_course_id_by_second_e = Window.partitionBy("seq_no", "course_id").orderBy(F.col("second_enrolled_at").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_seq_no_course_id_by_second_e_2 = Window.partitionBy("seq_no", "course_id").orderBy(F.col("second_enrolled_at").asc_nulls_last(), "id")

recent_enrollments = (
    df_enrollments
    .filter((F.col("student_id").isNotNull()) | (F.col("term") != "2025T2"))
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "enrolled_at",
    )
)

ranked = (
    recent_enrollments
    .select(
        "*",
        F.nth_value("enrolled_at", 2).over(w_all_by_score_full).alias("second_enrolled_at"),
        F.lag("score", 2).over(w_student_id_by_score).alias("prev2_score"),
        F.rank().over(w_all_by_enrolled_at_ties).alias("enrolled_at_rank"),
        F.row_number().over(w_student_id_by_score).alias("seq_no"),
        F.lag("enrolled_at").over(w_enrolled_at_to_date_student).alias("prev_enrolled_at"),
        F.lead("enrolled_at").over(w_student_id_by_score).alias("next_enrolled_at"),
        F.nth_value("score", 2).over(w_student_id_by_score_full).alias("second_score"),
        F.sum("score").over(w_student_id_course_id_by_enro).alias("score_sum_2_prior"),
        F.round(F.col("score") / F.nullif(F.sum("score").over(w_enrolled_at_year_term), F.lit(0)), 4).alias("score_share_of_enrolled_at_year_term"),
        F.lead("score").over(w_student_id_by_score).alias("next_score"),
        F.max("score").over(w_student_id).alias("student_id_max_score"),
        F.size(F.collect_set("course_id").over(w_enrolled_at_year_term)).alias("distinct_course_id_in_enrolled_at_year_term"),
        F.round(F.avg("score").over(w_enrolled_at_to_date_student_2), 2).alias("score_avg_1_prior"),
        F.nth_value("enrolled_at", 2).over(w_enrolled_at_to_date_student_3).alias("second_enrolled_at_2"),
    )
)

windowed = (
    ranked
    .select(
        "id",
        "student_id",
        "term",
        "enrolled_at",
        "seq_no",
        "prev_enrolled_at",
        "second_score",
        "score_sum_2_prior",
        "score_share_of_enrolled_at_year_term",
        "next_score",
        "student_id_max_score",
        "distinct_course_id_in_enrolled_at_year_term",
        "score_avg_1_prior",
        "second_enrolled_at_2",
        F.first("second_score", ignorenulls=True).over(w_seq_no_by_second_enrolled_at).alias("first_known_second_score"),
        F.first("seq_no").over(w_seq_no_course_id_by_second_e).alias("first_seq_no"),
        F.row_number().over(w_seq_no_course_id_by_second_e_2).alias("seq_no_2"),
    )
)

result = (
    windowed
    .select(
        "id",
        "student_id",
        "term",
        "enrolled_at",
        "seq_no",
        "second_score",
        "score_sum_2_prior",
        "score_share_of_enrolled_at_year_term",
        "next_score",
        "distinct_course_id_in_enrolled_at_year_term",
        "score_avg_1_prior",
        "second_enrolled_at_2",
        "first_known_second_score",
        "first_seq_no",
        (F.col("seq_no") + F.col("score_avg_1_prior")).alias("seq_no_plus_score_avg_1_prior"),
        F.coalesce(F.col("seq_no_2"), F.lit(0)).alias("seq_no_2_filled"),
    )
    .orderBy(
        F.col("seq_no").desc_nulls_first(),
        F.col("first_known_second_score").desc(),
    )
)
