from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_course_credits_filled_term = Window.partitionBy("course_credits_filled", "term")

cleaned = (
    df_enrollments
    .filter(F.col("score") >= 34.2)
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
        "enrolled_at",
        F.round(F.col("score") * 0.25, 2).alias("score_adj"),
    )
)

annotated = cleaned.withColumn("score_adj_plus_score", F.col("score_adj") + F.col("score"))

with_details_course = (
    annotated.alias("s")
    .join(df_courses.alias("c"), F.col("c.id") == F.col("s.course_id"), "inner")
    .select(
        "s.*",
        "c.course_code",
        F.col("c.level").alias("course_level"),
        F.col("c.credits").alias("course_credits"),
    )
)

labeled = (
    with_details_course
    .filter(F.col("score") <= 57.0)
    .select(
        "*",
        F.coalesce(F.col("course_credits"), F.lit(0)).alias("course_credits_filled"),
        F.least(F.col("score_adj"), F.col("course_credits")).alias("least_score_adj_course_credits"),
        F.length(F.trim(F.col("grade_status"))).alias("grade_status_len"),
    )
)

with_windows = (
    labeled
    .select(
        "*",
        F.count(F.lit(1)).over(w_course_credits_filled_term).alias("rows_in_course_credits_filled_term"),
        F.round(F.col("score_adj_plus_score") / F.nullif(F.sum("score_adj_plus_score").over(w_course_credits_filled_term), F.lit(0)), 4).alias("score_adj_plus_score_share_of_course_credits_fil"),
    )
)

stats = (
    with_windows
    .groupBy("course_credits")
    .agg(
        F.min("score_adj_plus_score").alias("min_score_adj_plus_score"),
        F.array_sort(F.collect_set("course_level")).alias("course_level_list"),
        F.round(F.sum("rows_in_course_credits_filled_term") / F.nullif(F.sum("score_adj_plus_score_share_of_course_credits_fil"), F.lit(0)), 3).alias("rows_in_course_credits_filled_term_to_score_adj_"),
        F.sum("grade_status_len").alias("total_grade_status_len"),
        F.sum("score_adj_plus_score_share_of_course_credits_fil").alias("sum_score_adj_plus_score_share_of_course_credits"),
    )
    .filter(F.col("sum_score_adj_plus_score_share_of_course_credits") > 0.1)
    .drop("sum_score_adj_plus_score_share_of_course_credits")
)

result = (
    stats
    .select("course_credits", "min_score_adj_plus_score")
    .orderBy(
        F.col("min_score_adj_plus_score").desc(),
        F.col("course_credits").desc(),
    )
)
