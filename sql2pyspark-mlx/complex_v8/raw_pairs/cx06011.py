from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_enrolled_at_moving_1 = Window.orderBy("enrolled_at", "id").rowsBetween(-1, Window.currentRow)
w_student_id = Window.partitionBy("student_id")
w_course_id_student_id_by_scor = Window.partitionBy("course_id", "student_id").orderBy(F.col("score").desc(), "id")
w_enrolled_at_to_date = Window.partitionBy(F.to_date("enrolled_at"))
w_enrolled_at_year_by_score_pr = Window.partitionBy(F.year("enrolled_at")).orderBy("score", "id").rowsBetween(-3, -1)
w_enrolled_at_year_by_score = Window.partitionBy(F.year("enrolled_at")).orderBy("score", "id")
w_course_id = Window.partitionBy("course_id")
w_course_id_term_by_enrolled_a = Window.partitionBy("course_id", "term").orderBy("enrolled_at", "id")

valid = (
    df_enrollments
    .filter(F.col("enrolled_at") < F.lit("2025-02-24 13:00:00").cast("timestamp"))
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "enrolled_at",
    )
)

windowed = (
    valid
    .select(
        "id",
        "course_id",
        "term",
        "score",
        "enrolled_at",
        F.max("score").over(w_all_by_enrolled_at_moving_1).alias("score_max_2"),
        F.round(F.avg("score").over(w_student_id), 2).alias("student_id_avg_score"),
        F.lag("enrolled_at", 3).over(w_course_id_student_id_by_scor).alias("prev3_enrolled_at"),
        F.count(F.lit(1)).over(w_student_id).alias("rows_in_student_id"),
        F.round(F.avg("score").over(w_enrolled_at_to_date), 2).alias("enrolled_at_to_date_avg_score"),
        F.round(F.col("score") - F.avg("score").over(w_student_id), 2).alias("score_vs_student_id_avg"),
        F.size(F.collect_set("student_id").over(w_enrolled_at_to_date)).alias("distinct_student_id_in_enrolled_at_to_date"),
        F.round(F.col("score") / F.nullif(F.sum("score").over(w_student_id), F.lit(0)), 4).alias("score_share_of_student_id"),
    )
)

with_windows = (
    windowed
    .select(
        "id",
        "course_id",
        "term",
        "score_max_2",
        "student_id_avg_score",
        "score_vs_student_id_avg",
        "distinct_student_id_in_enrolled_at_to_date",
        "score_share_of_student_id",
        F.sum("score_vs_student_id_avg").over(w_enrolled_at_year_by_score_pr).alias("score_vs_student_id_avg_sum_3_prior"),
        F.round(F.percent_rank().over(w_enrolled_at_year_by_score), 3).alias("score_percent_rank"),
        F.round(F.col("score_max_2") / F.nullif(F.sum("score_max_2").over(w_course_id), F.lit(0)), 4).alias("score_max_2_share_of_course_id"),
        F.size(F.collect_set("term").over(w_course_id)).alias("distinct_term_in_course_id"),
        F.rank().over(w_course_id_term_by_enrolled_a).alias("enrolled_at_rank"),
        F.row_number().over(w_enrolled_at_year_by_score).alias("seq_no"),
        F.count(F.lit(1)).over(w_course_id).alias("rows_in_course_id"),
    )
)

result = (
    with_windows
    .select(
        "id",
        "course_id",
        "term",
        "score_max_2",
        "student_id_avg_score",
        "distinct_student_id_in_enrolled_at_to_date",
        "score_share_of_student_id",
        "score_vs_student_id_avg_sum_3_prior",
        "score_max_2_share_of_course_id",
        "distinct_term_in_course_id",
        "enrolled_at_rank",
        "rows_in_course_id",
        F.lower(F.regexp_replace(F.trim(F.col("term")), " ", "_")).alias("term_slug"),
        F.greatest(F.col("score_vs_student_id_avg"), F.col("rows_in_course_id")).alias("greatest_score_vs_student_id_avg_rows_in_course_"),
    )
)
