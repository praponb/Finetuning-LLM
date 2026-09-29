from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_grade_status_len_movi = Window.orderBy("grade_status_len", "id").rowsBetween(-1, Window.currentRow)
w_all_by_grade_status_len_ties = Window.orderBy("grade_status_len")
w_course_id_term = Window.partitionBy("course_id", "term")

recent = (
    df_enrollments
    .filter((F.col("student_id").isNotNull()) & (F.hour(F.col("enrolled_at")).between(7, 13)))
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
        "enrolled_at",
        F.lower(F.regexp_replace(F.trim(F.col("grade_status")), " ", "_")).alias("grade_status_slug"),
        F.lower(F.regexp_replace(F.trim(F.col("term")), " ", "_")).alias("term_slug"),
        F.length(F.trim(F.col("grade_status"))).alias("grade_status_len"),
    )
)

course_id_stats = (
    recent
    .groupBy("course_id")
    .agg(F.avg("score").alias("avg_score"))
)

vs_group = (
    recent.alias("s")
    .join(course_id_stats.alias("g"), F.col("g.course_id") == F.col("s.course_id"), "inner")
    .filter(F.col("s.score") > F.col("g.avg_score"))
    .select(
        "s.*",
        "g.avg_score",
        F.round(F.col("s.score") - F.col("g.avg_score"), 2).alias("score_minus_avg"),
    )
)

ranked = (
    vs_group
    .select(
        "*",
        F.round(F.avg("grade_status_len").over(w_all_by_grade_status_len_movi), 2).alias("grade_status_len_avg_2"),
        F.rank().over(w_all_by_grade_status_len_ties).alias("grade_status_len_rank"),
        F.min("avg_score").over(w_course_id_term).alias("course_id_term_min_avg_score"),
    )
)

with_parent = (
    ranked.alias("s")
    .join(df_courses.alias("c"), F.col("c.id") == F.col("s.course_id"), "inner")
    .select(
        "s.*",
        F.col("c.level").alias("course_level"),
        F.col("c.credits").alias("course_credits"),
        "c.course_code",
    )
)

qualified = (
    with_parent.alias("s")
    .join(df_courses.alias("c"), (F.col("c.id") == F.col("s.course_id")) & (F.col("c.credits").between(2, 4)) & (F.col("c.dept").isNotNull()), "left_semi")
)

with_activity = (
    qualified
    .crossJoin(qualified.agg(F.avg("grade_status_len_avg_2").alias("overall_avg_grade_status_len_avg_2")))
    .filter(F.col("grade_status_len_avg_2") > F.col("overall_avg_grade_status_len_avg_2"))
    .drop("overall_avg_grade_status_len_avg_2")
)

result = (
    with_activity
    .select(
        "id",
        "student_id",
        "course_id",
        "score",
        "grade_status",
        "term_slug",
        "grade_status_len",
        "avg_score",
        "score_minus_avg",
        "grade_status_len_rank",
        "course_level",
        "course_credits",
        "course_code",
        F.when(F.col("grade_status_len") >= 12, "A").when(F.col("grade_status_len") >= 6, "B").otherwise("C").alias("grade_status_len_tier"),
    )
)
