from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_term_course_id_by_score = Window.partitionBy("term", "course_id").orderBy("score", "id")
w_all_by_score = Window.orderBy("score", "id")
w_term_course_id = Window.partitionBy("term", "course_id")
w_term_course_id_by_score_ties = Window.partitionBy("term", "course_id").orderBy("score")
w_course_id = Window.partitionBy("course_id")
w_term_course_id_by_score_runn = Window.partitionBy("term", "course_id").orderBy("score", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_grade_status = Window.partitionBy("grade_status")
w_student_id_by_score = Window.partitionBy("student_id").orderBy(F.col("score").desc(), "id")
w_grade_status_by_score = Window.partitionBy("grade_status").orderBy(F.col("score").desc(), "id")
w_grade_status_by_score_runnin = Window.partitionBy("grade_status").orderBy("score", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

prepared_enrollments = (
    df_enrollments
    .filter(F.col("student_id").isNotNull())
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
    )
)

windowed = (
    prepared_enrollments
    .select(
        "*",
        F.ntile(5).over(w_term_course_id_by_score).alias("score_quintile"),
        F.lead("score").over(w_all_by_score).alias("next_score"),
        F.round(F.col("score") - F.avg("score").over(w_term_course_id), 2).alias("score_vs_term_course_id_avg"),
        F.round(F.col("score") / F.nullif(F.sum("score").over(w_term_course_id), F.lit(0)), 4).alias("score_share_of_term_course_id"),
        F.dense_rank().over(w_term_course_id_by_score_ties).alias("score_dense_rank"),
        F.round(F.avg("score").over(w_course_id), 2).alias("course_id_avg_score"),
        F.count(F.lit(1)).over(w_term_course_id_by_score_runn).alias("running_count"),
        F.round(F.avg("score").over(w_term_course_id), 2).alias("term_course_id_avg_score"),
        F.round(F.avg("score").over(w_grade_status), 2).alias("grade_status_avg_score"),
        F.round(F.percent_rank().over(w_student_id_by_score), 3).alias("score_percent_rank"),
        F.lead("score").over(w_grade_status_by_score).alias("next_score_2"),
        F.last("score", ignorenulls=True).over(w_grade_status_by_score_runnin).alias("last_known_score"),
        F.round(F.col("score") - F.avg("score").over(w_course_id), 2).alias("score_vs_course_id_avg"),
    )
)

derived = (
    windowed
    .select(
        "id",
        "student_id",
        "course_id",
        "score_quintile",
        "next_score",
        "score_share_of_term_course_id",
        "term_course_id_avg_score",
        "grade_status_avg_score",
        "score_percent_rank",
        "next_score_2",
        "score_vs_course_id_avg",
        (F.col("score_quintile") + F.col("next_score")).alias("score_quintile_plus_next_score"),
        F.round(F.col("score_quintile") / F.nullif(F.col("running_count"), F.lit(0)), 3).alias("score_quintile_per_running_count"),
    )
)

result = (
    derived
    .select(
        "id",
        "student_id",
        "course_id",
        "score_quintile",
        "next_score",
        "score_share_of_term_course_id",
        "grade_status_avg_score",
        "score_percent_rank",
        "next_score_2",
        "score_quintile_plus_next_score",
        F.round(F.col("next_score_2") * 1.1, 2).alias("next_score_2_adj"),
        F.coalesce(F.col("term_course_id_avg_score"), F.lit(0.0)).alias("term_course_id_avg_score_filled"),
    )
    .orderBy(F.col("term_course_id_avg_score_filled").asc_nulls_last())
)
