from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_term = Window.partitionBy("term")
w_student_id = Window.partitionBy("student_id")
w_all_by_enrolled_at_running = Window.orderBy("enrolled_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_course_id_by_score_adj = Window.partitionBy("course_id").orderBy(F.col("score_adj").desc(), "id")
w_term_course_id_by_score_full = Window.partitionBy("term", "course_id").orderBy(F.col("score").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_term_course_id_by_score_runn = Window.partitionBy("term", "course_id").orderBy(F.col("score").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_term_course_id_by_score_prio = Window.partitionBy("term", "course_id").orderBy(F.col("score").desc(), "id").rowsBetween(-6, -1)
w_course_id = Window.partitionBy("course_id")
w_distinct_term_in_student_id = Window.partitionBy("distinct_term_in_student_id", F.to_date("enrolled_at")).orderBy("next_enrolled_at", "id")
w_distinct_term_in_student_id_2 = Window.partitionBy("distinct_term_in_student_id", "course_id").orderBy(F.col("running_score_adj").desc(), "id")

filtered = (
    df_enrollments
    .filter((F.col("course_id") <= 4) & (F.col("enrolled_at") >= F.lit("2025-02-12 09:00:00").cast("timestamp")))
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "enrolled_at",
        F.round(F.col("score") * 1.07, 2).alias("score_adj"),
    )
)

metrics = (
    filtered
    .select(
        "id",
        "course_id",
        "score",
        "enrolled_at",
        "score_adj",
        F.round(F.col("score_adj") / F.nullif(F.sum("score_adj").over(w_term), F.lit(0)), 4).alias("score_adj_share_of_term"),
        F.size(F.collect_set("term").over(w_student_id)).alias("distinct_term_in_student_id"),
        F.count(F.lit(1)).over(w_all_by_enrolled_at_running).alias("running_count"),
        F.sum("score_adj").over(w_student_id).alias("student_id_sum_score_adj"),
        F.lead("enrolled_at").over(w_course_id_by_score_adj).alias("next_enrolled_at"),
        F.first("score").over(w_term_course_id_by_score_full).alias("first_score"),
        F.sum("score_adj").over(w_term_course_id_by_score_runn).alias("running_score_adj"),
        F.sum("score_adj").over(w_term_course_id_by_score_prio).alias("score_adj_sum_6_prior"),
    )
)

metrics_2 = (
    metrics
    .select(
        "*",
        F.min("running_count").over(w_course_id).alias("course_id_min_running_count"),
        F.row_number().over(w_distinct_term_in_student_id).alias("rn"),
        (F.col("student_id_sum_score_adj") - F.lag("student_id_sum_score_adj").over(w_distinct_term_in_student_id_2)).alias("student_id_sum_score_adj_change"),
    )
)

result = (
    metrics_2
    .filter(F.col("student_id_sum_score_adj_change") < 1200.0)
    .select(
        "id",
        "score",
        "enrolled_at",
        "score_adj_share_of_term",
        "distinct_term_in_student_id",
        "running_count",
        "student_id_sum_score_adj",
        "next_enrolled_at",
        "first_score",
        "score_adj_sum_6_prior",
        "rn",
        "student_id_sum_score_adj_change",
    )
    .orderBy("student_id_sum_score_adj", F.col("next_enrolled_at").asc())
)
