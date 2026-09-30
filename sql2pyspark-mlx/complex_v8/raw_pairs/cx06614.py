from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_course_level_by_score_full = Window.partitionBy("course_level").orderBy("score", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_course_level_by_score_prior = Window.partitionBy("course_level").orderBy("score", "id").rowsBetween(-1, -1)
w_student_id_by_score = Window.partitionBy("student_id").orderBy(F.col("score").desc(), "id")
w_course_level_term = Window.partitionBy("course_level", "term")
w_all_by_score = Window.orderBy(F.col("score").desc(), "id")
w_term_course_id_by_score_movi = Window.partitionBy("term", "course_id").orderBy(F.col("score").desc_nulls_first(), "id").rowsBetween(-3, Window.currentRow)
w_term_course_level = Window.partitionBy("term", "course_level")
w_student_id_by_score_running = Window.partitionBy("student_id").orderBy(F.col("score").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_course_level_by_score_ties = Window.partitionBy("course_level").orderBy("score")
w_term_course_id_by_score = Window.partitionBy("term", "course_id").orderBy(F.col("score").desc_nulls_first(), "id")
w_term_course_id_by_score_movi_2 = Window.partitionBy("term", "course_id").orderBy(F.col("score").desc_nulls_first(), "id").rowsBetween(-1, Window.currentRow)

recent_enrollments = (
    df_enrollments
    .filter(F.col("term") == "2024T1")
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
        "enrolled_at",
        F.concat(F.col("term"), F.lit("#"), F.col("course_id").cast("string")).alias("term_ref"),
    )
)

enriched_course = (
    recent_enrollments.alias("s")
    .join(df_courses.alias("c"), F.col("c.id") == F.col("s.course_id"), "left")
    .select(
        "s.id",
        "s.student_id",
        "s.course_id",
        "s.term",
        "s.score",
        F.col("c.level").alias("course_level"),
    )
)

metrics = (
    enriched_course
    .select(
        "id",
        "course_id",
        "term",
        "score",
        "course_level",
        F.nth_value("score", 2).over(w_course_level_by_score_full).alias("second_score"),
        F.sum("score").over(w_course_level_by_score_prior).alias("score_sum_1_prior"),
        F.lead("score").over(w_student_id_by_score).alias("next_score"),
        F.size(F.collect_set("student_id").over(w_course_level_term)).alias("distinct_student_id_in_course_level_term"),
        (F.col("score") - F.lag("score").over(w_all_by_score)).alias("score_change"),
        F.max("score").over(w_term_course_id_by_score_movi).alias("score_max_4"),
        F.size(F.collect_set("course_id").over(w_term_course_level)).alias("distinct_course_id_in_term_course_level"),
        F.sum("score").over(w_student_id_by_score_running).alias("running_score"),
        F.max("score").over(w_course_level_term).alias("course_level_term_max_score"),
        F.round(F.percent_rank().over(w_course_level_by_score_ties), 3).alias("score_percent_rank"),
        F.lead("score").over(w_term_course_id_by_score).alias("next_score_2"),
        F.sum("score").over(w_term_course_id_by_score_movi_2).alias("score_sum_2"),
    )
)

scored = (
    metrics
    .filter(F.col("score_percent_rank") >= 0.2)
    .select(
        "*",
        (F.floor(F.col("next_score_2") / 25) * 25).alias("next_score_2_bucket"),
        F.abs(F.col("next_score") - 57.3).alias("next_score_distance"),
    )
)

result = (
    scored
    .select(
        "id",
        "course_id",
        "term",
        "score",
        "second_score",
        "score_sum_1_prior",
        "next_score",
        "distinct_student_id_in_course_level_term",
        "score_change",
        "score_max_4",
        "distinct_course_id_in_term_course_level",
        "running_score",
        "course_level_term_max_score",
        "score_percent_rank",
        "next_score_2",
        "next_score_2_bucket",
        "next_score_distance",
        F.least(F.col("score_change"), F.col("score_max_4")).alias("least_score_change_score_max_4"),
    )
    .orderBy(F.col("term").asc_nulls_last(), "id")
    .limit(10)
)
