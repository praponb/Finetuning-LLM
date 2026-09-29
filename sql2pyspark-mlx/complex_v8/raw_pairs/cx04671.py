from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_course_level_by_score_full = Window.partitionBy("course_level").orderBy("score", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_course_id_course_credits_by = Window.partitionBy("course_id", "course_credits").orderBy("course_credits")
w_course_id_course_credits_by_2 = Window.partitionBy("course_id", "course_credits").orderBy("course_credits", "id").rowsBetween(-2, -1)
w_course_level_by_score = Window.partitionBy("course_level").orderBy("score", "id")
w_term_student_id_by_score_pri = Window.partitionBy("term", "student_id").orderBy("score", "id").rowsBetween(-1, -1)
w_course_credits_course_id = Window.partitionBy("course_credits", "course_id")
w_term_student_id_by_score_tie = Window.partitionBy("term", "student_id").orderBy("score")
w_all_by_course_credits_cume_d = Window.orderBy("course_credits_cume_dist", "id")
w_prev2_course_credits_course = Window.partitionBy("prev2_course_credits", "course_credits").orderBy("score_dense_rank")
w_course_id_rn = Window.partitionBy("course_id", "rn")
w_all_by_course_credits_cume_d_2 = Window.orderBy("course_credits_cume_dist", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_distinct_terms_rn_2_by_score = Window.partitionBy("distinct_terms", "rn_2").orderBy("score_course_level_core", "rn_2")
w_distinct_terms_rn_2 = Window.partitionBy("distinct_terms", "rn_2")
w_distinct_terms_rn_2_by_score_2 = Window.partitionBy("distinct_terms", "rn_2").orderBy("score_course_level_core")

recent = (
    df_enrollments
    .filter(F.col("score") >= 6.2)
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
        F.substring(F.trim(F.col("term")), 1, 3).alias("term_prefix"),
    )
)

with_details_course = (
    recent.alias("s")
    .join(df_courses.alias("c"), F.col("c.id") == F.col("s.course_id"), "left")
    .select(
        "s.id",
        "s.student_id",
        "s.course_id",
        "s.term",
        "s.score",
        "s.grade_status",
        "c.course_code",
        F.col("c.credits").alias("course_credits"),
        F.col("c.level").alias("course_level"),
    )
)

metrics = (
    with_details_course
    .select(
        "*",
        F.first("course_credits").over(w_course_level_by_score_full).alias("first_course_credits"),
        F.round(F.cume_dist().over(w_course_id_course_credits_by), 3).alias("course_credits_cume_dist"),
        F.round(F.avg("course_credits").over(w_course_id_course_credits_by_2), 2).alias("course_credits_avg_2_prior"),
        F.lag("course_credits", 2).over(w_course_level_by_score).alias("prev2_course_credits"),
        F.round(F.avg("course_credits").over(w_term_student_id_by_score_pri), 2).alias("course_credits_avg_1_prior"),
        F.round(F.avg("course_credits").over(w_course_credits_course_id), 2).alias("course_credits_course_id_avg_course_credits"),
        F.dense_rank().over(w_term_student_id_by_score_tie).alias("score_dense_rank"),
        F.count(F.lit(1)).over(w_course_credits_course_id).alias("rows_in_course_credits_course_id"),
        F.row_number().over(w_course_level_by_score).alias("rn"),
    )
)

with_windows = (
    metrics
    .select(
        "id",
        "term",
        "score",
        "course_level",
        "first_course_credits",
        "course_credits_cume_dist",
        "score_dense_rank",
        "rows_in_course_credits_course_id",
        "rn",
        F.row_number().over(w_all_by_course_credits_cume_d).alias("rn_2"),
        F.lead("course_credits").over(w_all_by_course_credits_cume_d).alias("next_course_credits"),
        F.round(F.cume_dist().over(w_prev2_course_credits_course), 3).alias("score_dense_rank_cume_dist"),
        F.max("first_course_credits").over(w_course_id_rn).alias("course_id_rn_max_first_course_credits"),
        F.first("score", ignorenulls=True).over(w_all_by_course_credits_cume_d_2).alias("first_known_score"),
        F.round(F.avg("prev2_course_credits").over(w_course_id_rn), 2).alias("course_id_rn_avg_prev2_course_credits"),
    )
)

grouped = (
    with_windows
    .groupBy("rn_2")
    .agg(
        F.sum(F.when(F.col("course_level") != "core", F.col("score")).otherwise(0)).alias("score_course_level_core"),
        F.min("first_known_score").alias("min_first_known_score"),
        F.array_sort(F.collect_set("course_level")).alias("course_level_list"),
        F.countDistinct("term").alias("distinct_terms"),
    )
)

sequenced = (
    grouped
    .select(
        "*",
        F.rank().over(w_distinct_terms_rn_2_by_score).alias("score_course_level_core_rank"),
        F.round(F.avg("min_first_known_score").over(w_distinct_terms_rn_2), 2).alias("distinct_terms_rn_2_avg_min_first_known_score"),
        F.lag("rn_2", 1, 0).over(w_distinct_terms_rn_2_by_score).alias("prev_rn_2"),
        F.rank().over(w_distinct_terms_rn_2_by_score_2).alias("score_course_level_core_rank_2"),
    )
)

result = (
    sequenced
    .select(
        "rn_2",
        "distinct_terms",
        "distinct_terms_rn_2_avg_min_first_known_score",
        "score_course_level_core_rank_2",
        F.round(F.col("score_course_level_core") / F.nullif(F.col("prev_rn_2"), F.lit(0)), 3).alias("score_course_level_core_per_prev_rn_2"),
    )
    .orderBy(
        F.col("score_course_level_core_per_prev_rn_2").asc(),
        F.col("distinct_terms").asc(),
    )
)
