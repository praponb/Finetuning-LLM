from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_enrolled_at_year_course_id_b = Window.partitionBy(F.year("enrolled_at"), "course_id").orderBy("score", "id")
w_student_id_grade_status_by_e = Window.partitionBy("student_id", "grade_status").orderBy(F.col("enrolled_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_course_id_grade_status_by_en = Window.partitionBy("course_id", "grade_status").orderBy("enrolled_at", "id")
w_student_id_grade_status_by_e_2 = Window.partitionBy("student_id", "grade_status").orderBy(F.col("enrolled_at").desc(), "id")
w_student_id_enrolled_at_month = Window.partitionBy("student_id", F.date_trunc("month", "enrolled_at")).orderBy(F.col("score").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_enrolled_at_year_course_id_b_2 = Window.partitionBy(F.year("enrolled_at"), "course_id").orderBy("score", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_enrolled_at_year_course_id_b_3 = Window.partitionBy(F.year("enrolled_at"), "course_id").orderBy("score", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_course_id_grade_status_by_en_2 = Window.partitionBy("course_id", "grade_status").orderBy("enrolled_at", "id").rowsBetween(-3, -1)

scoped = (
    df_enrollments
    .filter(F.col("student_id") >= 4)
    .select(
        "id",
        "student_id",
        "course_id",
        "score",
        "grade_status",
        "enrolled_at",
        F.concat_ws(": ", F.col("term"), F.col("grade_status")).alias("term_grade_status_label"),
    )
)

metrics = (
    scoped
    .select(
        "*",
        F.lag("score").over(w_enrolled_at_year_course_id_b).alias("prev_score"),
        F.row_number().over(w_enrolled_at_year_course_id_b).alias("seq_no"),
        F.nth_value("enrolled_at", 2).over(w_student_id_grade_status_by_e).alias("second_enrolled_at"),
        F.rank().over(w_course_id_grade_status_by_en).alias("enrolled_at_rank"),
        F.ntile(10).over(w_student_id_grade_status_by_e_2).alias("enrolled_at_decile"),
        F.first("score", ignorenulls=True).over(w_student_id_enrolled_at_month).alias("first_known_score"),
        F.nth_value("score", 2).over(w_enrolled_at_year_course_id_b_2).alias("second_score"),
        F.count(F.lit(1)).over(w_enrolled_at_year_course_id_b_3).alias("running_count"),
        F.sum("score").over(w_course_id_grade_status_by_en_2).alias("score_sum_3_prior"),
    )
)

scored = (
    metrics
    .select(
        "*",
        F.abs(F.col("score_sum_3_prior") - 180.0).alias("score_sum_3_prior_distance"),
        F.concat_ws(": ", F.col("grade_status"), F.col("term_grade_status_label")).alias("grade_status_term_grade_status_label_label"),
        F.coalesce(F.col("grade_status"), F.lit("UNKNOWN")).alias("grade_status_or_unknown"),
    )
)

result = (
    scored
    .select(
        "id",
        "student_id",
        "score",
        "grade_status",
        "enrolled_at",
        "term_grade_status_label",
        "prev_score",
        "seq_no",
        "enrolled_at_rank",
        "enrolled_at_decile",
        "first_known_score",
        "second_score",
        "running_count",
        "score_sum_3_prior",
        "grade_status_or_unknown",
        F.abs(F.col("running_count") - 8).alias("running_count_distance"),
        F.round(F.col("first_known_score") / F.nullif(F.col("running_count"), F.lit(0)), 3).alias("first_known_score_per_running_count"),
    )
    .orderBy(
        F.col("grade_status").asc(),
        F.col("term_grade_status_label").asc_nulls_last(),
    )
)
