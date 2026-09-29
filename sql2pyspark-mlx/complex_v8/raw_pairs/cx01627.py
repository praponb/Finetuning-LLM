from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_course_id = Window.partitionBy("course_id")

base_enrollments = (
    df_enrollments
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
        "enrolled_at",
        F.date_format(F.col("enrolled_at"), "MMM yyyy").alias("enrolled_at_label"),
        F.initcap(F.trim(F.col("term"))).alias("term_clean"),
    )
)

without_matches = (
    base_enrollments.alias("s")
    .join(df_enrollments.alias("e"), (F.col("e.student_id") == F.col("s.student_id")) & (F.col("e.grade_status").isin("WITHDRAWN", "INCOMPLETE", "PASS")) & (F.col("e.score") > 11.1), "left_semi")
    .filter(F.col("term").isin("2024T1", "2024T2"))
)

with_trends = (
    without_matches
    .select(
        "id",
        "course_id",
        "term",
        "score",
        F.max("score").over(w_course_id).alias("course_id_max_score"),
        F.round(F.col("score") / F.nullif(F.sum("score").over(w_course_id), F.lit(0)), 4).alias("score_share_of_course_id"),
        F.min("score").over(w_course_id).alias("course_id_min_score"),
    )
)

course_id_stats = (
    with_trends
    .groupBy("course_id")
    .agg(F.percentile_approx("score_share_of_course_id", 0.5).alias("median_score_share_of_course_id"))
)

compared = (
    with_trends.alias("s")
    .join(course_id_stats.alias("g"), F.col("g.course_id") == F.col("s.course_id"), "inner")
    .filter(F.col("s.score_share_of_course_id") >= F.col("g.median_score_share_of_course_id"))
    .select(
        "s.*",
        "g.median_score_share_of_course_id",
        F.round(F.col("s.score_share_of_course_id") - F.col("g.median_score_share_of_course_id"), 2).alias("score_share_of_course_id_minus_median"),
    )
)

qualified = (
    compared.alias("s")
    .join(df_enrollments.alias("e"), (F.col("e.course_id") == F.col("s.course_id")) & (F.col("e.grade_status").isin("WITHDRAWN", "FAIL", "INCOMPLETE")), "left_semi")
)

with_parent = (
    qualified.alias("s")
    .join(df_courses.alias("c"), F.col("c.id") == F.col("s.course_id"), "left")
    .select(
        "s.id",
        "s.course_id",
        "s.term",
        "s.median_score_share_of_course_id",
        "s.score_share_of_course_id_minus_median",
        F.col("c.credits").alias("course_credits"),
        F.col("c.level").alias("course_level"),
        F.col("c.dept").alias("course_dept"),
    )
)

screened = (
    with_parent
    .crossJoin(with_parent.agg(F.avg("median_score_share_of_course_id").alias("overall_avg_median_score_share_of_course_id")))
    .filter(F.col("median_score_share_of_course_id") > F.col("overall_avg_median_score_share_of_course_id"))
    .drop("overall_avg_median_score_share_of_course_id")
)

result = (
    screened
    .select(
        "id",
        "course_id",
        "term",
        "median_score_share_of_course_id",
        "score_share_of_course_id_minus_median",
        "course_credits",
        "course_level",
        "course_dept",
        F.coalesce(F.col("course_credits"), F.lit(0)).alias("course_credits_filled"),
        F.when(F.col("course_credits") >= 3, "HIGH").when(F.col("course_credits") >= 2, "MEDIUM").otherwise("LOW").alias("course_credits_tier"),
    )
    .orderBy(F.col("course_dept").desc_nulls_first(), "id")
    .limit(20)
)
