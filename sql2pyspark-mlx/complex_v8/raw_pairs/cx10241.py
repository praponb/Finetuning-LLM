from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_term_course_id_by_score = Window.partitionBy("term", "course_id").orderBy("score", "id")
w_term_course_id_by_score_prio = Window.partitionBy("term", "course_id").orderBy("score", "id").rowsBetween(-1, -1)
w_term_course_id_by_score_ties = Window.partitionBy("term", "course_id").orderBy("score")
w_all_by_score = Window.orderBy("score", "id")
w_grade_status = Window.partitionBy("grade_status")
w_all_by_student_enrolled_on_f = Window.orderBy("student_enrolled_on", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_term_course_id_by_score_prio_2 = Window.partitionBy("term", "course_id").orderBy("score", "id").rowsBetween(-2, -1)

recent = (
    df_enrollments
    .filter(F.col("student_id").isNotNull())
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
        "enrolled_at",
        F.coalesce(F.col("score"), F.lit(0.0)).alias("score_filled"),
    )
)

with_parent = (
    recent.alias("s")
    .join(df_courses.alias("c"), F.col("c.id") == F.col("s.course_id"), "inner")
    .join(df_students.alias("st"), F.col("st.id") == F.col("s.student_id"), "left")
    .select(
        "s.*",
        F.col("c.level").alias("course_level"),
        F.col("st.enrolled_on").alias("student_enrolled_on"),
        F.col("st.cohort").alias("student_cohort"),
    )
)

sequenced = (
    with_parent
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "grade_status",
        "score_filled",
        "student_cohort",
        F.lead("student_enrolled_on").over(w_term_course_id_by_score).alias("next_student_enrolled_on"),
        F.round(F.avg("score_filled").over(w_term_course_id_by_score_prio), 2).alias("score_filled_avg_1_prior"),
        F.dense_rank().over(w_term_course_id_by_score_ties).alias("score_dense_rank"),
        F.round(F.cume_dist().over(w_term_course_id_by_score), 3).alias("score_cume_dist"),
        F.round(F.percent_rank().over(w_all_by_score), 3).alias("score_percent_rank"),
        F.size(F.collect_set("course_level").over(w_grade_status)).alias("distinct_course_level_in_grade_status"),
        F.last("enrolled_at").over(w_all_by_student_enrolled_on_f).alias("last_enrolled_at"),
        F.count(F.lit(1)).over(w_grade_status).alias("rows_in_grade_status"),
        F.round(F.col("score_filled") / F.nullif(F.sum("score_filled").over(w_grade_status), F.lit(0)), 4).alias("score_filled_share_of_grade_status"),
        F.round(F.col("score") / F.nullif(F.sum("score").over(w_grade_status), F.lit(0)), 4).alias("score_share_of_grade_status"),
        F.sum("score").over(w_term_course_id_by_score_prio_2).alias("score_sum_2_prior"),
    )
)

result = (
    sequenced
    .select(
        "id",
        "course_id",
        "term",
        "student_cohort",
        "score_percent_rank",
        "distinct_course_level_in_grade_status",
        "rows_in_grade_status",
        "score_filled_share_of_grade_status",
        "score_share_of_grade_status",
        "score_sum_2_prior",
        F.date_trunc("quarter", F.col("last_enrolled_at")).alias("last_enrolled_at_quarter"),
        F.date_trunc("week", F.col("next_student_enrolled_on")).alias("next_student_enrolled_on_week"),
    )
    .orderBy("next_student_enrolled_on_week")
)
