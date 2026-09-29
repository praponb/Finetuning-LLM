from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_course_id_student_id_by_scor = Window.partitionBy("course_id", "student_id").orderBy("score", "id")
w_course_id = Window.partitionBy("course_id")
w_all_by_score_ties = Window.orderBy(F.col("score").asc_nulls_last())
w_student_id = Window.partitionBy("student_id")
w_all_by_score = Window.orderBy(F.col("score").asc_nulls_last(), "id")
w_all_by_score_running = Window.orderBy(F.col("score").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_score_ties_2 = Window.orderBy(F.col("score").desc())
w_course_id_student_id_by_scor_2 = Window.partitionBy("course_id", "student_id").orderBy("score", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_score_dense_rank_by_score_qu = Window.partitionBy("score_dense_rank").orderBy("score_quartile_2", "id").rowsBetween(-6, Window.currentRow)
w_all_by_least_score_last_know = Window.orderBy(F.col("least_score_last_known_score").desc(), "id")
w_all_by_score_ties_3 = Window.orderBy("score")
w_all_by_score_running_2 = Window.orderBy("score", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_score_dense_rank_by_score_qu_2 = Window.partitionBy("score_dense_rank").orderBy("score_quartile_2", "id").rowsBetween(-1, Window.currentRow)
w_all_by_least_score_last_know_2 = Window.orderBy(F.col("least_score_last_known_score").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_score_dense_rank = Window.partitionBy("score_dense_rank")

base = df_enrollments.filter((F.col("score") >= 35.8) | (F.col("term") != "2024T1"))

with_parent_course = (
    base.alias("s")
    .join(df_courses.alias("c"), F.col("c.id") == F.col("s.course_id"), "inner")
    .filter(F.upper(F.trim(F.col("c.course_code"))).like("C%"))
    .select(
        "s.id",
        "s.student_id",
        "s.course_id",
        "s.score",
        "c.course_code",
    )
)

ranked = (
    with_parent_course
    .select(
        "*",
        F.ntile(4).over(w_course_id_student_id_by_scor).alias("score_quartile"),
        F.count(F.lit(1)).over(w_course_id).alias("rows_in_course_id"),
        F.lead("score").over(w_course_id_student_id_by_scor).alias("next_score"),
        F.dense_rank().over(w_all_by_score_ties).alias("score_dense_rank"),
        F.size(F.collect_set("course_id").over(w_student_id)).alias("distinct_course_id_in_student_id"),
        F.ntile(4).over(w_all_by_score).alias("score_quartile_2"),
        F.first("score", ignorenulls=True).over(w_all_by_score_running).alias("first_known_score"),
        F.round(F.percent_rank().over(w_all_by_score_ties_2), 3).alias("score_percent_rank"),
        F.round(F.avg("score").over(w_course_id), 2).alias("course_id_avg_score"),
        F.last("score", ignorenulls=True).over(w_course_id_student_id_by_scor_2).alias("last_known_score"),
    )
)

scored = (
    ranked
    .select(
        "id",
        "student_id",
        "course_id",
        "score",
        "score_quartile",
        "next_score",
        "score_dense_rank",
        "score_quartile_2",
        "first_known_score",
        "score_percent_rank",
        "last_known_score",
        F.least(F.col("score"), F.col("last_known_score")).alias("least_score_last_known_score"),
    )
)

windowed = (
    scored
    .select(
        "*",
        F.round(F.avg("score_dense_rank").over(w_score_dense_rank_by_score_qu), 2).alias("score_dense_rank_avg_7"),
        F.lag("last_known_score", 2).over(w_all_by_least_score_last_know).alias("prev2_last_known_score"),
        F.dense_rank().over(w_all_by_score_ties_3).alias("score_dense_rank_2"),
        F.first("score_quartile_2", ignorenulls=True).over(w_all_by_score_running_2).alias("first_known_score_quartile_2"),
        F.round(F.avg("least_score_last_known_score").over(w_score_dense_rank_by_score_qu_2), 2).alias("least_score_last_known_score_avg_2"),
        F.sum("score_quartile").over(w_all_by_least_score_last_know_2).alias("running_score_quartile"),
        F.sum("first_known_score").over(w_course_id).alias("course_id_sum_first_known_score"),
        F.sum("score_percent_rank").over(w_score_dense_rank).alias("score_dense_rank_sum_score_percent_rank"),
    )
)

result = (
    windowed
    .select(
        "id",
        "student_id",
        "course_id",
        "score",
        "next_score",
        "score_dense_rank",
        "score_quartile_2",
        "first_known_score",
        "score_percent_rank",
        "least_score_last_known_score",
        "score_dense_rank_avg_7",
        "prev2_last_known_score",
        "score_dense_rank_2",
        "first_known_score_quartile_2",
        "least_score_last_known_score_avg_2",
        "running_score_quartile",
        "course_id_sum_first_known_score",
        "score_dense_rank_sum_score_percent_rank",
        (F.floor(F.col("first_known_score") / 25) * 25).alias("first_known_score_bucket"),
    )
    .orderBy(F.col("running_score_quartile").desc(), F.col("id").asc_nulls_last())
)
