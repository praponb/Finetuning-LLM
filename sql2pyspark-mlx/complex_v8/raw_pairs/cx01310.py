from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_credits_enrollment_term_by_e = Window.partitionBy("credits", "enrollment_term").orderBy("enrollment_score_distance", "id", "enrollment_id")
w_credits_enrollment_term_by_e_2 = Window.partitionBy("credits", "enrollment_term").orderBy("enrollment_score_distance", "id", "enrollment_id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_credits_enrollment_term_by_e_3 = Window.partitionBy("credits", "enrollment_term").orderBy("enrollment_score_distance")

filtered = (
    df_courses
    .filter((F.col("dept") == "Art") | (F.col("credits") < 3))
    .select(
        "id",
        "course_code",
        "dept",
        "credits",
        "level",
        F.abs(F.col("credits") - 3).alias("credits_distance"),
        F.lower(F.regexp_replace(F.trim(F.col("dept")), " ", "_")).alias("dept_slug"),
        F.lower(F.regexp_replace(F.trim(F.col("course_code")), " ", "_")).alias("course_code_slug"),
    )
)

line_level_enrollments = (
    filtered.alias("s")
    .join(df_enrollments.alias("e"), F.col("e.course_id") == F.col("s.id"), "inner")
    .select(
        "s.id",
        "s.course_code",
        "s.dept",
        "s.credits",
        "s.level",
        "s.dept_slug",
        "s.course_code_slug",
        F.col("e.id").alias("enrollment_id"),
        F.col("e.score").alias("enrollment_score"),
        F.col("e.enrolled_at").alias("enrollment_enrolled_at"),
        F.col("e.term").alias("enrollment_term"),
        F.col("e.student_id").alias("enrollment_student_id"),
    )
)

scored = (
    line_level_enrollments
    .filter(F.col("course_code_slug").isNotNull())
    .select(
        "*",
        F.concat_ws("|", F.col("level"), F.col("dept_slug")).alias("level_dept_slug_label"),
        F.abs(F.col("enrollment_score") - 49.0).alias("enrollment_score_distance"),
        F.initcap(F.trim(F.col("level"))).alias("level_clean"),
    )
)

with_trends = (
    scored
    .select(
        "id",
        "course_code",
        "dept",
        "credits",
        "dept_slug",
        "course_code_slug",
        "enrollment_id",
        "enrollment_score",
        "enrollment_student_id",
        "level_dept_slug_label",
        "enrollment_score_distance",
        "level_clean",
        F.ntile(10).over(w_credits_enrollment_term_by_e).alias("enrollment_score_distance_decile"),
        F.nth_value("enrollment_enrolled_at", 2).over(w_credits_enrollment_term_by_e_2).alias("second_enrollment_enrolled_at"),
        F.first("credits").over(w_credits_enrollment_term_by_e_2).alias("first_credits"),
        F.round(F.percent_rank().over(w_credits_enrollment_term_by_e_3), 3).alias("enrollment_score_distance_percent_rank"),
    )
)

summary = (
    with_trends
    .groupBy("first_credits")
    .agg(
        F.countDistinct("id").alias("distinct_ids"),
        F.array_sort(F.collect_set("dept")).alias("dept_list"),
        F.count("*").alias("row_count"),
        F.round(F.stddev("enrollment_score_distance_decile"), 2).alias("stddev_enrollment_score_distance_decile"),
    )
)

filtered_2 = (
    summary
    .crossJoin(summary.agg(F.avg("first_credits").alias("overall_avg_first_credits")))
    .filter(F.col("first_credits") > F.col("overall_avg_first_credits"))
    .drop("overall_avg_first_credits")
)

result = (
    filtered_2
    .select(
        "first_credits",
        "distinct_ids",
        "row_count",
        "stddev_enrollment_score_distance_decile",
    )
    .orderBy(
        F.col("stddev_enrollment_score_distance_decile").desc(),
        F.col("distinct_ids").desc_nulls_first(),
        "first_credits",
    )
    .limit(10)
)
