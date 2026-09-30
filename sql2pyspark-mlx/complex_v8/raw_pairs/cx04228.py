from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_cnt_term = Window.partitionBy("cnt", "term")
w_cnt_by_cnt_ties = Window.partitionBy("cnt").orderBy("cnt")

scoped = (
    df_enrollments
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

grade_status_stats = (
    scoped
    .groupBy("grade_status")
    .agg(F.avg("score").alias("avg_score"))
)

vs_group = (
    scoped.alias("s")
    .join(grade_status_stats.alias("g"), F.col("g.grade_status") == F.col("s.grade_status"), "inner")
    .filter(F.col("s.score") > F.col("g.avg_score"))
    .select(
        "s.*",
        "g.avg_score",
        F.round(F.col("s.score") - F.col("g.avg_score"), 2).alias("score_minus_avg"),
    )
)

with_details_course = (
    vs_group.alias("s")
    .join(df_courses.alias("c"), (F.col("c.id") == F.col("s.course_id")) & (F.col("c.credits") > 2), "left")
    .select(
        "s.id",
        "s.student_id",
        "s.course_id",
        "s.term",
        "s.score",
        "s.grade_status",
        "s.score_minus_avg",
        "c.course_code",
    )
)

totals = (
    with_details_course
    .groupBy("term", "grade_status")
    .agg(
        F.array_sort(F.collect_set("term")).alias("term_list"),
        F.count("*").alias("cnt"),
    )
    .filter(F.col("cnt") >= 2)
)

sequenced = (
    totals
    .select(
        "term",
        "grade_status",
        "term_list",
        "cnt",
        F.round(F.avg("cnt").over(w_cnt_term), 2).alias("cnt_term_avg_cnt"),
        F.dense_rank().over(w_cnt_by_cnt_ties).alias("cnt_dense_rank"),
        F.size(F.collect_set("grade_status").over(w_cnt_term)).alias("distinct_grade_status_in_cnt_term"),
        F.round(F.col("cnt") - F.avg("cnt").over(w_cnt_term), 2).alias("cnt_vs_cnt_term_avg"),
    )
)

result = (
    sequenced
    .groupingSets(
        [["distinct_grade_status_in_cnt_term", "term", "cnt_dense_rank"], ["distinct_grade_status_in_cnt_term"], []],
        "distinct_grade_status_in_cnt_term", "term", "cnt_dense_rank",
    )
    .agg(
        F.grouping_id().alias("gid"),
        F.min("cnt_dense_rank").alias("min_cnt_dense_rank"),
        F.array_sort(F.collect_set("term")).alias("term_list_2"),
        F.count("*").alias("cnt_2"),
    )
    .select(
        "distinct_grade_status_in_cnt_term",
        F.coalesce(F.col("term"), F.lit("ALL")).alias("term"),
        "cnt_dense_rank",
        "gid",
        "min_cnt_dense_rank",
        "term_list_2",
        "cnt_2",
    )
)
