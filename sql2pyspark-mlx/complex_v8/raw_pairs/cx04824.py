from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_term = Window.partitionBy("term")
w_course_id_grade_status = Window.partitionBy("course_id", "grade_status")
w_enrolled_at_to_date_by_score = Window.partitionBy(F.to_date("enrolled_at")).orderBy("score", "id")
w_course_id_term_by_enrolled_a = Window.partitionBy("course_id", "term").orderBy(F.col("enrolled_at").desc(), "id")
w_course_id_term_by_enrolled_a_2 = Window.partitionBy("course_id", "term").orderBy(F.col("enrolled_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_course_id_grade_status_by_en = Window.partitionBy("course_id", "grade_status").orderBy("enrolled_at", "id")
w_enrolled_at_to_date_by_score_2 = Window.partitionBy(F.to_date("enrolled_at")).orderBy("score", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_course_id_grade_status_by_en_2 = Window.partitionBy("course_id", "grade_status").orderBy("enrolled_at")
w_student_id_by_enrolled_at_pr = Window.partitionBy("student_id").orderBy(F.col("enrolled_at").desc(), "id").rowsBetween(-1, -1)
w_student_id_by_enrolled_at_pr_2 = Window.partitionBy("student_id").orderBy("enrolled_at", "id").rowsBetween(-2, -1)
w_grade_status_course_id = Window.partitionBy("grade_status", "course_id")
w_term_by_score_share_of_term = Window.partitionBy("term").orderBy("score_share_of_term", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_term_by_score_share_of_term_2 = Window.partitionBy("term").orderBy("score_share_of_term", "id")

recent_enrollments = (
    df_enrollments
    .filter((F.col("score") < 59.8) & (~F.col("term").isin("2025T2", "2025T1")))
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

metrics = (
    recent_enrollments
    .select(
        "*",
        F.size(F.collect_set("student_id").over(w_term)).alias("distinct_student_id_in_term"),
        F.size(F.collect_set("term").over(w_course_id_grade_status)).alias("distinct_term_in_course_id_grade_status"),
        F.lead("enrolled_at", 3).over(w_enrolled_at_to_date_by_score).alias("next3_enrolled_at"),
        F.round(F.col("score") - F.avg("score").over(w_course_id_grade_status), 2).alias("score_vs_course_id_grade_status_avg"),
        F.row_number().over(w_course_id_term_by_enrolled_a).alias("enrolled_at_seq"),
        F.max("score").over(w_course_id_term_by_enrolled_a_2).alias("max_score_so_far"),
        F.lead("enrolled_at").over(w_course_id_grade_status_by_en).alias("next_enrolled_at"),
        F.count(F.lit(1)).over(w_course_id_grade_status).alias("rows_in_course_id_grade_status"),
        F.last("score", ignorenulls=True).over(w_enrolled_at_to_date_by_score_2).alias("last_known_score"),
        F.round(F.percent_rank().over(w_course_id_grade_status_by_en_2), 3).alias("enrolled_at_percent_rank"),
        F.round(F.col("score") / F.nullif(F.sum("score").over(w_term), F.lit(0)), 4).alias("score_share_of_term"),
        F.round(F.avg("score").over(w_student_id_by_enrolled_at_pr), 2).alias("score_avg_1_prior"),
        F.round(F.avg("score").over(w_student_id_by_enrolled_at_pr_2), 2).alias("score_avg_2_prior"),
        F.count(F.lit(1)).over(w_term).alias("rows_in_term"),
    )
)

metrics_2 = (
    metrics
    .select(
        "*",
        F.round(F.col("score_avg_2_prior") - F.avg("score_avg_2_prior").over(w_grade_status_course_id), 2).alias("score_avg_2_prior_vs_grade_status_course_id_avg"),
        F.max("score_avg_2_prior").over(w_grade_status_course_id).alias("grade_status_course_id_max_score_avg_2_prior"),
        F.last("enrolled_at_percent_rank", ignorenulls=True).over(w_term_by_score_share_of_term).alias("last_known_enrolled_at_percent_rank"),
        F.round((F.unix_timestamp("enrolled_at") - F.unix_timestamp(F.lag("enrolled_at").over(w_term_by_score_share_of_term_2))) / 60, 1).alias("minutes_since_prev_enrolled_at"),
        F.last("score_avg_2_prior", ignorenulls=True).over(w_term_by_score_share_of_term).alias("last_known_score_avg_2_prior"),
    )
)

result = (
    metrics_2
    .select(
        "id",
        "course_id",
        "term",
        "score",
        "grade_status",
        "enrolled_at",
        "distinct_student_id_in_term",
        "next3_enrolled_at",
        "score_vs_course_id_grade_status_avg",
        "enrolled_at_seq",
        "max_score_so_far",
        "rows_in_course_id_grade_status",
        "last_known_score",
        "enrolled_at_percent_rank",
        "score_share_of_term",
        "score_avg_1_prior",
        "score_avg_2_prior",
        "rows_in_term",
        "score_avg_2_prior_vs_grade_status_course_id_avg",
        "grade_status_course_id_max_score_avg_2_prior",
        "last_known_enrolled_at_percent_rank",
        "last_known_score_avg_2_prior",
    )
    .orderBy(
        "last_known_enrolled_at_percent_rank",
        F.col("score_avg_2_prior_vs_grade_status_course_id_avg").desc(),
    )
)
