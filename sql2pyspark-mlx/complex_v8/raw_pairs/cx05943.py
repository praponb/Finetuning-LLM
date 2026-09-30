from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_enrolled_at = Window.orderBy(F.col("enrolled_at").desc_nulls_first(), "id")
w_term_student_id = Window.partitionBy("term", "student_id")
w_all_by_enrolled_at_prior_2 = Window.orderBy(F.col("enrolled_at").desc_nulls_first(), "id").rowsBetween(-2, -1)
w_all_by_enrolled_at_full = Window.orderBy(F.col("enrolled_at").desc_nulls_first(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_student_id_term_by_last_seco = Window.partitionBy("student_id", "term").orderBy(F.col("last_second_enrolled_at").desc(), "second_enrolled_at_month", "term", "student_id")
w_all_by_min_rows_in_term_stud = Window.orderBy("min_rows_in_term_student_id", "second_enrolled_at_month", "term", "student_id")
w_all_by_min_rows_in_term_stud_2 = Window.orderBy("min_rows_in_term_student_id")

prepared_enrollments = (
    df_enrollments
    .filter(F.col("course_id").isNotNull())
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
        "enrolled_at",
        F.substring(F.trim(F.col("term")), 1, 2).alias("term_prefix"),
        F.lower(F.trim(F.col("grade_status"))).alias("grade_status_clean"),
    )
)

sequenced = (
    prepared_enrollments
    .select(
        "*",
        F.ntile(5).over(w_all_by_enrolled_at).alias("enrolled_at_quintile"),
        F.count(F.lit(1)).over(w_term_student_id).alias("rows_in_term_student_id"),
        F.max("score").over(w_all_by_enrolled_at_prior_2).alias("score_max_2_prior"),
        F.nth_value("enrolled_at", 2).over(w_all_by_enrolled_at_full).alias("second_enrolled_at"),
    )
)

per_group = (
    sequenced
    .groupBy(
        F.date_trunc("month", "second_enrolled_at").alias("second_enrolled_at_month"),
        "term",
        "student_id",
    )
    .agg(
        F.min("rows_in_term_student_id").alias("min_rows_in_term_student_id"),
        F.max("second_enrolled_at").alias("last_second_enrolled_at"),
        F.max("score_max_2_prior").alias("max_score_max_2_prior"),
        F.count("*").alias("n_rows"),
        F.array_sort(F.collect_set("grade_status")).alias("grade_status_list"),
    )
)

with_trends = (
    per_group
    .select(
        "*",
        F.lead("last_second_enrolled_at").over(w_student_id_term_by_last_seco).alias("next_last_second_enrolled_at"),
        F.round((F.unix_timestamp("last_second_enrolled_at") - F.unix_timestamp(F.lag("last_second_enrolled_at").over(w_all_by_min_rows_in_term_stud))) / 60, 1).alias("minutes_since_prev_last_second_enrolled_at"),
        F.round(F.percent_rank().over(w_all_by_min_rows_in_term_stud_2), 3).alias("min_rows_in_term_student_id_percent_rank"),
        (F.col("n_rows") - F.lag("n_rows").over(w_all_by_min_rows_in_term_stud)).alias("n_rows_change"),
    )
)

per_label = (
    with_trends
    .select(
        "second_enrolled_at_month",
        "term",
        "student_id",
        "min_rows_in_term_student_id",
        "last_second_enrolled_at",
        "max_score_max_2_prior",
        "n_rows",
        "next_last_second_enrolled_at",
        "minutes_since_prev_last_second_enrolled_at",
        "min_rows_in_term_student_id_percent_rank",
        "n_rows_change",
        F.explode("grade_status_list").alias("grade_status_list_item"),
    )
)

result = (
    per_label
    .select(
        "second_enrolled_at_month",
        "min_rows_in_term_student_id",
        "last_second_enrolled_at",
        "max_score_max_2_prior",
        "n_rows",
        "next_last_second_enrolled_at",
        "min_rows_in_term_student_id_percent_rank",
        "n_rows_change",
        "grade_status_list_item",
        F.least(F.col("n_rows_change"), F.col("max_score_max_2_prior")).alias("least_n_rows_change_max_score_max_2_prior"),
    )
    .orderBy(
        F.col("max_score_max_2_prior").desc(),
        F.col("last_second_enrolled_at").desc_nulls_first(),
    )
)
