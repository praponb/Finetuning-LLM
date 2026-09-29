from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_student_id = Window.partitionBy("student_id")
w_course_id_term_by_score_adj = Window.partitionBy("course_id", "term").orderBy("score_adj", "id").rowsBetween(-1, Window.currentRow)
w_course_id_term_by_score_adj_2 = Window.partitionBy("course_id", "term").orderBy("score_adj", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_enrolled_at_ties = Window.orderBy("enrolled_at")
w_student_id_term_by_score = Window.partitionBy("student_id", "term").orderBy(F.col("score").desc(), "id")
w_all_by_enrolled_at = Window.orderBy("enrolled_at", "id")
w_course_id_term_by_score_adj_3 = Window.partitionBy("course_id", "term").orderBy("score_adj", "id")

recent = (
    df_enrollments
    .filter(F.month(F.col("enrolled_at")).isin(1, 2))
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
        "enrolled_at",
        F.concat(F.col("grade_status"), F.lit("#"), F.col("id").cast("string")).alias("grade_status_ref"),
        F.round(F.col("score") * 0.9, 2).alias("score_adj"),
    )
)

windowed = (
    recent
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
        "grade_status_ref",
        F.count(F.lit(1)).over(w_student_id).alias("rows_in_student_id"),
        F.sum("score_adj").over(w_course_id_term_by_score_adj).alias("score_adj_sum_2"),
        F.size(F.collect_set("grade_status").over(w_student_id)).alias("distinct_grade_status_in_student_id"),
        F.first("score_adj", ignorenulls=True).over(w_course_id_term_by_score_adj_2).alias("first_known_score_adj"),
        F.round(F.percent_rank().over(w_all_by_enrolled_at_ties), 3).alias("enrolled_at_percent_rank"),
        F.min("score_adj").over(w_student_id).alias("student_id_min_score_adj"),
        F.lead("score_adj").over(w_student_id_term_by_score).alias("next_score_adj"),
        F.sum("score").over(w_student_id).alias("student_id_sum_score"),
        F.ntile(5).over(w_all_by_enrolled_at).alias("enrolled_at_quintile"),
        F.sum("score").over(w_course_id_term_by_score_adj_2).alias("running_score"),
        F.round(F.percent_rank().over(w_student_id_term_by_score), 3).alias("score_percent_rank"),
        F.lead("score", 3).over(w_all_by_enrolled_at).alias("next3_score"),
        (F.col("score_adj") - F.lag("score_adj").over(w_course_id_term_by_score_adj_3)).alias("score_adj_change"),
    )
)

result = (
    windowed
    .filter(F.col("student_id_min_score_adj") > 525.0)
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status_ref",
        "rows_in_student_id",
        "score_adj_sum_2",
        "distinct_grade_status_in_student_id",
        "first_known_score_adj",
        "enrolled_at_percent_rank",
        "student_id_min_score_adj",
        "next_score_adj",
        "student_id_sum_score",
        "enrolled_at_quintile",
        "running_score",
        "score_percent_rank",
        "score_adj_change",
        F.when(F.col("enrolled_at_percent_rank") >= 0.8, "A").when(F.col("enrolled_at_percent_rank") >= 0.5, "B").otherwise("C").alias("enrolled_at_percent_rank_tier"),
        (F.floor(F.col("first_known_score_adj") / 25) * 25).alias("first_known_score_adj_bucket"),
    )
    .orderBy(
        F.col("enrolled_at_percent_rank").asc_nulls_last(),
        F.col("student_id_sum_score").desc(),
        "id",
    )
    .limit(10)
)
