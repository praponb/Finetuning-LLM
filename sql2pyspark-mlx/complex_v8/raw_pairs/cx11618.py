from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_student_major_student_id = Window.partitionBy("student_major", "student_id")
w_course_id_term = Window.partitionBy("course_id", "term")
w_all_by_score = Window.orderBy(F.col("score").desc(), "id")
w_student_major_term_by_score = Window.partitionBy("student_major", "term").orderBy("score")
w_grade_status_by_score_runnin = Window.partitionBy("grade_status").orderBy(F.col("score").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_score_prior_3 = Window.orderBy(F.col("score").desc(), "id").rowsBetween(-3, -1)
w_student_major_by_score_runni = Window.partitionBy("student_major").orderBy("score", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_student_major_course_id = Window.partitionBy("student_major", "course_id")
w_distinct_term_in_student_maj = Window.partitionBy("distinct_term_in_student_major_student_id", "term")
w_term_course_id_by_score_shar = Window.partitionBy("term", "course_id").orderBy("score_share_of_student_major_student_id")
w_student_major_course_id_by_r = Window.partitionBy("student_major", "course_id").orderBy(F.col("running_score").asc_nulls_last(), "id")
w_term_course_id = Window.partitionBy("term", "course_id")

valid_enrollments = (
    df_enrollments
    .filter(F.col("score") > 11.3)
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
    )
)

with_parent = (
    valid_enrollments.alias("s")
    .join(df_students.alias("st"), F.col("st.id") == F.col("s.student_id"), "left")
    .select(
        "s.id",
        "s.student_id",
        "s.course_id",
        "s.term",
        "s.score",
        "s.grade_status",
        F.col("st.major").alias("student_major"),
    )
)

sequenced = (
    with_parent
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "student_major",
        F.size(F.collect_set("term").over(w_student_major_student_id)).alias("distinct_term_in_student_major_student_id"),
        F.round(F.col("score") / F.nullif(F.sum("score").over(w_course_id_term), F.lit(0)), 4).alias("score_share_of_course_id_term"),
        F.lag("score").over(w_all_by_score).alias("prev_score"),
        F.lead("score").over(w_all_by_score).alias("next_score"),
        F.dense_rank().over(w_all_by_score).alias("score_dense_rank"),
        F.round(F.percent_rank().over(w_student_major_term_by_score), 3).alias("score_percent_rank"),
        (F.col("score") - F.lag("score").over(w_all_by_score)).alias("score_change"),
        F.size(F.collect_set("course_id").over(w_student_major_student_id)).alias("distinct_course_id_in_student_major_student_id"),
        F.sum("score").over(w_grade_status_by_score_runnin).alias("running_score"),
        F.round(F.col("score") / F.nullif(F.sum("score").over(w_student_major_student_id), F.lit(0)), 4).alias("score_share_of_student_major_student_id"),
        F.round(F.avg("score").over(w_all_by_score_prior_3), 2).alias("score_avg_3_prior"),
        F.min("score").over(w_course_id_term).alias("course_id_term_min_score"),
        F.count(F.lit(1)).over(w_student_major_student_id).alias("rows_in_student_major_student_id"),
        F.first("score", ignorenulls=True).over(w_student_major_by_score_runni).alias("first_known_score"),
        F.round(F.col("score") - F.avg("score").over(w_student_major_course_id), 2).alias("score_vs_student_major_course_id_avg"),
    )
)

leaders = sequenced.filter(F.col("score_dense_rank") <= 3)

with_windows = (
    leaders
    .select(
        "*",
        F.round(F.col("next_score") / F.nullif(F.sum("next_score").over(w_distinct_term_in_student_maj), F.lit(0)), 4).alias("next_score_share_of_distinct_term_in_student_maj"),
        F.round(F.percent_rank().over(w_term_course_id_by_score_shar), 3).alias("score_share_of_student_major_student_id_percent_"),
        (F.col("running_score") - F.lag("running_score").over(w_student_major_course_id_by_r)).alias("running_score_change"),
        F.round(F.avg("score_percent_rank").over(w_distinct_term_in_student_maj), 2).alias("distinct_term_in_student_major_student_id_term_a"),
        F.count(F.lit(1)).over(w_term_course_id).alias("rows_in_term_course_id"),
        F.min("score_avg_3_prior").over(w_term_course_id).alias("term_course_id_min_score_avg_3_prior"),
    )
)

result = (
    with_windows
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "distinct_term_in_student_major_student_id",
        "score_share_of_course_id_term",
        "next_score",
        "score_dense_rank",
        "score_percent_rank",
        "score_change",
        "distinct_course_id_in_student_major_student_id",
        "running_score",
        "score_share_of_student_major_student_id",
        "score_avg_3_prior",
        "course_id_term_min_score",
        "rows_in_student_major_student_id",
        "first_known_score",
        "next_score_share_of_distinct_term_in_student_maj",
        "running_score_change",
        "rows_in_term_course_id",
        F.coalesce(F.col("score"), F.lit(0.0)).alias("score_filled"),
        F.concat(F.col("term"), F.lit("#"), F.col("rows_in_term_course_id").cast("string")).alias("term_ref"),
    )
)
