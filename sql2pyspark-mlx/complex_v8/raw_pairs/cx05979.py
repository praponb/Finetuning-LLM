from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_score_tier_course_dept = Window.partitionBy("score_tier", "course_dept")
w_grade_status_term_label_len = Window.partitionBy("grade_status_term_label_len").orderBy(F.col("score").desc())

cleaned_enrollments = (
    df_enrollments
    .filter((F.col("course_id") >= 3) & (F.col("score") <= 74.9))
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
        F.year(F.col("enrolled_at")).alias("enrolled_at_year"),
        F.concat_ws(" / ", F.col("grade_status"), F.col("term")).alias("grade_status_term_label"),
        F.round(F.col("score") * 0.9, 2).alias("score_adj"),
    )
)

combined_details_course = (
    cleaned_enrollments.alias("s")
    .join(df_courses.alias("c"), (F.col("c.id") == F.col("s.course_id")) & (F.col("c.dept") == "CS"), "inner")
    .select(
        "s.id",
        "s.student_id",
        "s.course_id",
        "s.score",
        "s.grade_status",
        "s.grade_status_term_label",
        F.col("c.dept").alias("course_dept"),
    )
)

screened = (
    combined_details_course.alias("s")
    .join(df_enrollments.alias("e"), F.col("e.student_id") == F.col("s.student_id"), "left_semi")
)

derived = (
    screened
    .filter(F.col("grade_status").isin("FAIL", "WITHDRAWN"))
    .select(
        "id",
        "score",
        "grade_status",
        "course_dept",
        F.length(F.trim(F.col("grade_status_term_label"))).alias("grade_status_term_label_len"),
        F.concat(F.col("grade_status_term_label"), F.lit("#"), F.col("course_id").cast("string")).alias("grade_status_term_label_ref"),
        F.when(F.col("score") >= 69.6, "large").when(F.col("score") >= 30.0, "medium").otherwise("small").alias("score_tier"),
    )
)

metrics = (
    derived
    .select(
        "*",
        F.round(F.col("grade_status_term_label_len") - F.avg("grade_status_term_label_len").over(w_score_tier_course_dept), 2).alias("grade_status_term_label_len_vs_score_tier_course"),
        F.round(F.cume_dist().over(w_grade_status_term_label_len), 3).alias("score_cume_dist"),
    )
)

without_matches = (
    metrics
    .crossJoin(metrics.agg(F.max("grade_status_term_label_len_vs_score_tier_course").alias("overall_max_grade_status_term_label_len_vs_score")))
    .filter(F.col("grade_status_term_label_len_vs_score_tier_course") < F.col("overall_max_grade_status_term_label_len_vs_score"))
    .drop("overall_max_grade_status_term_label_len_vs_score")
)

result = (
    without_matches
    .filter(F.col("score_tier").isin("large", "small"))
    .select(
        "id",
        "score",
        "course_dept",
        "grade_status_term_label_ref",
        "grade_status_term_label_len_vs_score_tier_course",
        F.round(F.col("score_cume_dist") / F.nullif(F.col("score"), F.lit(0)), 3).alias("score_cume_dist_per_score"),
    )
    .orderBy(
        F.col("grade_status_term_label_len_vs_score_tier_course").desc(),
        "grade_status_term_label_ref",
    )
)
