from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_grade_status_by_score_prior = Window.partitionBy("grade_status").orderBy(F.col("score").asc_nulls_last(), "id").rowsBetween(-6, -1)
w_grade_status_by_score = Window.partitionBy("grade_status").orderBy(F.col("score").asc_nulls_last(), "id")
w_grade_status = Window.partitionBy("grade_status")
w_grade_status_by_score_full = Window.partitionBy("grade_status").orderBy(F.col("score").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_term_grade_status_by_score = Window.partitionBy("term", "grade_status").orderBy("score", "id")
w_course_id_grade_status = Window.partitionBy("course_id", "grade_status")
w_grade_status_by_score_moving = Window.partitionBy("grade_status").orderBy(F.col("score").asc_nulls_last(), "id").rowsBetween(-6, Window.currentRow)
w_grade_status_by_score_ties = Window.partitionBy("grade_status").orderBy(F.col("score").asc_nulls_last())
w_distinct_student_id_in_grade = Window.partitionBy("distinct_student_id_in_grade_status")
w_all_by_rows_in_course_id_gra = Window.orderBy("rows_in_course_id_grade_status", "id").rowsBetween(-6, Window.currentRow)
w_all_by_score_vs_course_id_gr = Window.orderBy("score_vs_course_id_grade_status_avg", "id")
w_all_by_score_vs_course_id_gr_2 = Window.orderBy("score_vs_course_id_grade_status_avg")

cleaned = (
    df_enrollments
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
        "enrolled_at",
        F.when(F.col("term").isin("2024T2", "2025T2"), "group_a").when(F.col("term") == "2025T1", "group_b").otherwise("other").alias("term_group"),
        F.to_date(F.col("enrolled_at")).alias("enrolled_at_date"),
    )
)

ranked = (
    cleaned
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        F.max("score").over(w_grade_status_by_score_prior).alias("score_max_6_prior"),
        F.row_number().over(w_grade_status_by_score).alias("rn"),
        F.count(F.lit(1)).over(w_grade_status).alias("rows_in_grade_status"),
        F.nth_value("enrolled_at", 2).over(w_grade_status_by_score_full).alias("second_enrolled_at"),
        F.lead("enrolled_at_date", 2).over(w_grade_status_by_score).alias("next2_enrolled_at_date"),
        F.size(F.collect_set("student_id").over(w_grade_status)).alias("distinct_student_id_in_grade_status"),
        (F.col("score") - F.lag("score").over(w_term_grade_status_by_score)).alias("score_change"),
        F.count(F.lit(1)).over(w_course_id_grade_status).alias("rows_in_course_id_grade_status"),
        F.round(F.avg("score").over(w_grade_status_by_score_moving), 2).alias("score_avg_7"),
        F.dense_rank().over(w_grade_status_by_score_ties).alias("score_dense_rank"),
        F.round(F.col("score") - F.avg("score").over(w_course_id_grade_status), 2).alias("score_vs_course_id_grade_status_avg"),
    )
)

derived = (
    ranked
    .filter(F.col("score_max_6_prior") <= 240.0)
    .select(
        "*",
        F.initcap(F.trim(F.col("term"))).alias("term_clean"),
        F.lower(F.regexp_replace(F.trim(F.col("term")), " ", "_")).alias("term_slug"),
    )
)

with_windows = (
    derived
    .select(
        "*",
        F.round(F.col("rows_in_grade_status") - F.avg("rows_in_grade_status").over(w_distinct_student_id_in_grade), 2).alias("rows_in_grade_status_vs_distinct_student_id_in_g"),
        F.round(F.avg("score_avg_7").over(w_all_by_rows_in_course_id_gra), 2).alias("score_avg_7_avg_7"),
        F.ntile(10).over(w_all_by_score_vs_course_id_gr).alias("score_vs_course_id_grade_status_avg_decile"),
        F.dense_rank().over(w_all_by_score_vs_course_id_gr_2).alias("score_vs_course_id_grade_status_avg_dense_rank"),
        F.round(F.col("score_change") / F.nullif(F.sum("score_change").over(w_distinct_student_id_in_grade), F.lit(0)), 4).alias("score_change_share_of_distinct_student_id_in_gra"),
    )
)

result = (
    with_windows
    .select(
        "id",
        "student_id",
        "course_id",
        "rn",
        "rows_in_grade_status",
        "second_enrolled_at",
        "next2_enrolled_at_date",
        "distinct_student_id_in_grade_status",
        "score_change",
        "score_avg_7",
        "score_dense_rank",
        "score_vs_course_id_grade_status_avg",
        "score_avg_7_avg_7",
        "score_vs_course_id_grade_status_avg_decile",
        "score_vs_course_id_grade_status_avg_dense_rank",
        "score_change_share_of_distinct_student_id_in_gra",
        F.coalesce(F.col("term"), F.lit("UNKNOWN")).alias("term_or_unknown"),
    )
    .orderBy(F.col("course_id").asc(), "id")
    .limit(5)
)
