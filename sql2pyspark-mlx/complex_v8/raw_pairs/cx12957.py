from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_grade_status_term_by_score = Window.partitionBy("grade_status", "term").orderBy(F.col("score").desc(), "id")
w_all_by_score = Window.orderBy(F.col("score").desc(), "id")
w_grade_status_term_by_score_p = Window.partitionBy("grade_status", "term").orderBy(F.col("score").desc(), "id").rowsBetween(-3, -1)
w_student_id = Window.partitionBy("student_id")
w_grade_status_term_by_score_t = Window.partitionBy("grade_status", "term").orderBy(F.col("score").desc())
w_all_by_score_full = Window.orderBy(F.col("score").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_score_ties = Window.orderBy("score")
w_all_by_score_prior_2 = Window.orderBy(F.col("score").desc(), "id").rowsBetween(-2, -1)
w_student_id_grade_status = Window.partitionBy("student_id", "grade_status")
w_score_dense_rank_score_rank = Window.partitionBy("score_dense_rank", "score_rank").orderBy(F.col("score_avg_2_prior").desc(), "id").rowsBetween(-6, -1)
w_score_tercile = Window.partitionBy("score_tercile")
w_score_dense_rank_score_rank_2 = Window.partitionBy("score_dense_rank", "score_rank").orderBy(F.col("score_avg_2_prior").desc(), "id")
w_term_score_tercile_by_score = Window.partitionBy("term", "score_tercile").orderBy("score_rank", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

base_enrollments = (
    df_enrollments
    .filter(F.col("score") < 80.2)
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
        F.substring(F.trim(F.col("term")), 1, 2).alias("term_prefix"),
    )
)

sequenced = (
    base_enrollments
    .select(
        "id",
        "course_id",
        "term",
        "score",
        "term_prefix",
        F.ntile(3).over(w_grade_status_term_by_score).alias("score_tercile"),
        (F.col("score") - F.lag("score").over(w_all_by_score)).alias("score_change"),
        F.round(F.avg("score").over(w_grade_status_term_by_score_p), 2).alias("score_avg_3_prior"),
        F.round(F.col("score") / F.nullif(F.sum("score").over(w_student_id), F.lit(0)), 4).alias("score_share_of_student_id"),
        F.dense_rank().over(w_grade_status_term_by_score_t).alias("score_dense_rank"),
        F.first("score").over(w_all_by_score_full).alias("first_score"),
        F.rank().over(w_all_by_score_ties).alias("score_rank"),
        F.round(F.avg("score").over(w_all_by_score_prior_2), 2).alias("score_avg_2_prior"),
        F.round(F.col("score") / F.nullif(F.sum("score").over(w_student_id_grade_status), F.lit(0)), 4).alias("score_share_of_student_id_grade_status"),
    )
)

top_per_group = sequenced.filter((F.col("score_dense_rank") == 1) & (F.col("score_change") >= -10.0))

windowed = (
    top_per_group
    .select(
        "*",
        F.max("score_rank").over(w_score_dense_rank_score_rank).alias("score_rank_max_6_prior"),
        F.size(F.collect_set("course_id").over(w_score_tercile)).alias("distinct_course_id_in_score_tercile"),
        F.count(F.lit(1)).over(w_score_tercile).alias("rows_in_score_tercile"),
        F.lead("score_rank", 3).over(w_score_dense_rank_score_rank_2).alias("next3_score_rank"),
        F.min("first_score").over(w_term_score_tercile_by_score).alias("min_first_score_so_far"),
        F.size(F.collect_set("term").over(w_score_tercile)).alias("distinct_term_in_score_tercile"),
    )
)

result = (
    windowed
    .filter(F.col("score_tercile").between(4, 7))
    .select(
        "id",
        "term",
        "score",
        "term_prefix",
        "score_tercile",
        "score_change",
        "score_avg_3_prior",
        "score_share_of_student_id",
        "score_dense_rank",
        "first_score",
        "score_rank",
        "score_avg_2_prior",
        "score_share_of_student_id_grade_status",
        "score_rank_max_6_prior",
        "distinct_course_id_in_score_tercile",
        "rows_in_score_tercile",
        "next3_score_rank",
        "distinct_term_in_score_tercile",
        F.lower(F.regexp_replace(F.trim(F.col("term")), " ", "_")).alias("term_slug"),
        F.lower(F.regexp_replace(F.trim(F.col("term")), " ", "_")).alias("term_slug_2"),
    )
    .orderBy(
        F.col("score_change").asc_nulls_last(),
        F.col("distinct_course_id_in_score_tercile").desc(),
    )
)
