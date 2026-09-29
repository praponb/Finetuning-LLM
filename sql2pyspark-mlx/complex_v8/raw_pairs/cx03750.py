from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_student_id_by_score_distance = Window.partitionBy("student_id").orderBy(F.col("score_distance").desc_nulls_first(), "id").rowsBetween(-6, Window.currentRow)
w_student_id_by_score_distance_2 = Window.partitionBy("student_id").orderBy(F.col("score_distance").desc_nulls_first(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

scoped = (
    df_enrollments
    .filter(F.col("score") < 85.2)
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
        F.when(F.col("term").isin("2024T2", "2025T2"), "group_a").when(F.col("term") == "2025T1", "group_b").otherwise("other").alias("term_group"),
        (F.floor(F.col("score") / 25) * 25).alias("score_bucket"),
        F.when(F.col("grade_status").isin("PASS", "INCOMPLETE"), "group_a").when(F.col("grade_status") == "WITHDRAWN", "group_b").otherwise("other").alias("grade_status_group"),
    )
)

enriched = (
    scoped
    .select(
        "id",
        "student_id",
        "course_id",
        "term_group",
        F.when(F.col("score_bucket") >= 70, "HIGH").when(F.col("score_bucket") >= 25, "MEDIUM").otherwise("LOW").alias("score_bucket_tier"),
        F.when(F.col("score_bucket") >= 65, "HIGH").when(F.col("score_bucket") >= 45, "MEDIUM").otherwise("LOW").alias("score_bucket_tier_2"),
        F.abs(F.col("score") - 63.9).alias("score_distance"),
    )
)

sequenced = (
    enriched
    .select(
        "id",
        "student_id",
        "score_bucket_tier",
        "score_bucket_tier_2",
        F.sum("score_distance").over(w_student_id_by_score_distance).alias("score_distance_sum_7"),
        F.nth_value("score_distance", 2).over(w_student_id_by_score_distance_2).alias("second_score_distance"),
    )
)

detail_rows_enrollments = (
    sequenced.alias("s")
    .join(df_enrollments.alias("e"), (F.col("e.student_id") == F.col("s.student_id")) & (F.col("e.course_id").isNotNull()), "inner")
    .select(
        "s.id",
        "s.student_id",
        "s.score_bucket_tier",
        "s.score_bucket_tier_2",
        "s.second_score_distance",
        F.col("e.id").alias("enrollment_id"),
        F.col("e.score").alias("enrollment_score"),
        F.col("e.course_id").alias("enrollment_course_id"),
    )
)

qualified = (
    detail_rows_enrollments
    .crossJoin(detail_rows_enrollments.agg(F.max("enrollment_score").alias("overall_max_enrollment_score")))
    .filter(F.col("enrollment_score") < F.col("overall_max_enrollment_score"))
    .drop("overall_max_enrollment_score")
)

with_parent = (
    qualified.alias("s")
    .join(df_courses.alias("c"), (F.col("c.id") == F.col("s.enrollment_course_id")) & (F.col("c.dept") != "Bio"), "inner")
    .join(df_enrollments.alias("e"), F.col("e.id") == F.col("s.enrollment_id"), "inner")
    .select(
        "s.id",
        "s.student_id",
        "s.score_bucket_tier",
        "s.score_bucket_tier_2",
        "s.second_score_distance",
        "s.enrollment_id",
        "s.enrollment_score",
        "s.enrollment_course_id",
        F.col("c.credits").alias("course_credits"),
        "c.course_code",
        F.col("e.score").alias("enrollment_score_2"),
    )
)

result = (
    with_parent
    .filter(F.col("student_id").isNotNull())
    .select(
        "id",
        "student_id",
        "score_bucket_tier",
        "score_bucket_tier_2",
        "second_score_distance",
        "enrollment_id",
        "enrollment_score",
        "enrollment_course_id",
        "course_code",
        F.when(F.col("score_bucket_tier_2").isin("HIGH", "LOW"), "group_a").when(F.col("score_bucket_tier_2") == "MEDIUM", "group_b").otherwise("other").alias("score_bucket_tier_2_group"),
        F.lower(F.regexp_replace(F.trim(F.col("score_bucket_tier")), " ", "_")).alias("score_bucket_tier_slug"),
    )
    .orderBy(F.col("score_bucket_tier_2").asc())
)
