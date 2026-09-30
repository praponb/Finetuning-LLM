from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_grade_status_term = Window.partitionBy("grade_status", "term")
w_all_by_enrolled_at_prior_1 = Window.orderBy("enrolled_at", "id").rowsBetween(-1, -1)
w_course_id_by_enrolled_at_tie = Window.partitionBy("course_id").orderBy("enrolled_at")
w_term_student_id = Window.partitionBy("term", "student_id")
w_term_by_enrolled_at_ties = Window.partitionBy("term").orderBy(F.col("enrolled_at").desc())
w_grade_status_course_id = Window.partitionBy("grade_status", "course_id")
w_course_id_enrolled_at_to_dat = Window.partitionBy("course_id", F.to_date("enrolled_at")).orderBy("enrolled_at", "id")
w_term_by_enrolled_at_moving_1 = Window.partitionBy("term").orderBy(F.col("enrolled_at").desc(), "id").rowsBetween(-1, Window.currentRow)
w_student_id = Window.partitionBy("student_id")
w_term_distinct_grade_status_i = Window.partitionBy("term", "distinct_grade_status_in_term_student_id").orderBy(F.col("grade_status_term_min_score").desc(), "id").rowsBetween(-1, Window.currentRow)
w_enrolled_at_month_distinct_g = Window.partitionBy(F.month("enrolled_at"), "distinct_grade_status_in_term_student_id").orderBy("score_vs_grade_status_course_id_avg", "id")

base_enrollments = df_enrollments.filter(F.col("student_id").isNotNull())

metrics = (
    base_enrollments
    .select(
        "*",
        F.min("score").over(w_grade_status_term).alias("grade_status_term_min_score"),
        F.max("score").over(w_all_by_enrolled_at_prior_1).alias("score_max_1_prior"),
        F.rank().over(w_course_id_by_enrolled_at_tie).alias("enrolled_at_rank"),
        F.count(F.lit(1)).over(w_grade_status_term).alias("rows_in_grade_status_term"),
        F.size(F.collect_set("grade_status").over(w_term_student_id)).alias("distinct_grade_status_in_term_student_id"),
        F.round(F.percent_rank().over(w_term_by_enrolled_at_ties), 3).alias("enrolled_at_percent_rank"),
        F.round(F.col("score") / F.nullif(F.sum("score").over(w_grade_status_term), F.lit(0)), 4).alias("score_share_of_grade_status_term"),
        F.round(F.col("score") - F.avg("score").over(w_grade_status_course_id), 2).alias("score_vs_grade_status_course_id_avg"),
        (F.col("score") - F.lag("score").over(w_course_id_enrolled_at_to_dat)).alias("score_change"),
        F.max("score").over(w_term_by_enrolled_at_moving_1).alias("score_max_2"),
    )
)

top_per_group = metrics.filter(F.col("enrolled_at_rank") <= 5)

metrics_2 = (
    top_per_group
    .select(
        "*",
        F.round(F.avg("enrolled_at_percent_rank").over(w_student_id), 2).alias("student_id_avg_enrolled_at_percent_rank"),
        F.max("enrolled_at_rank").over(w_term_distinct_grade_status_i).alias("enrolled_at_rank_max_2"),
        F.lead("enrolled_at_percent_rank").over(w_enrolled_at_month_distinct_g).alias("next_enrolled_at_percent_rank"),
        F.ntile(5).over(w_enrolled_at_month_distinct_g).alias("score_vs_grade_status_course_id_avg_quintile"),
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
        "grade_status_term_min_score",
        "score_max_1_prior",
        "distinct_grade_status_in_term_student_id",
        "enrolled_at_percent_rank",
        "score_share_of_grade_status_term",
        "score_vs_grade_status_course_id_avg",
        "score_change",
        "student_id_avg_enrolled_at_percent_rank",
        "enrolled_at_rank_max_2",
        "score_vs_grade_status_course_id_avg_quintile",
        F.round(F.col("score_change") / F.nullif(F.col("enrolled_at_percent_rank"), F.lit(0)), 3).alias("score_change_per_enrolled_at_percent_rank"),
        F.to_date(F.col("enrolled_at")).alias("enrolled_at_date"),
    )
    .orderBy(F.col("score_max_1_prior").desc())
)
