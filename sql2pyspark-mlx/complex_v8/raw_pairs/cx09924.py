from pyspark.sql import functions as F

scoped_enrollments = (
    df_enrollments
    .filter((F.col("score") >= 42.4) | (F.month(F.col("enrolled_at")).isin(1, 3)))
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
        "enrolled_at",
        F.when(F.col("term").isin("2025T1", "2024T1"), "group_a").when(F.col("term") == "2024T2", "group_b").otherwise("other").alias("term_group"),
        F.when(F.col("term").isin("2024T1", "2025T2"), "group_a").when(F.col("term") == "2025T1", "group_b").otherwise("other").alias("term_group_2"),
        F.substring(F.trim(F.col("grade_status")), 1, 3).alias("grade_status_prefix"),
    )
)

qualified = (
    scoped_enrollments.alias("s")
    .join(df_enrollments.alias("e"), (F.col("e.student_id") == F.col("s.student_id")) & (F.col("e.course_id").isNotNull()), "left_semi")
    .filter(F.col("grade_status") == "FAIL")
)

with_parent = (
    qualified.alias("s")
    .join(df_courses.alias("c"), F.col("c.id") == F.col("s.course_id"), "left")
    .select(
        "s.id",
        "s.student_id",
        "s.course_id",
        "s.term",
        "s.score",
        "s.grade_status",
        "s.enrolled_at",
        "s.term_group",
        "s.grade_status_prefix",
        F.col("c.credits").alias("course_credits"),
        F.col("c.dept").alias("course_dept"),
        "c.course_code",
    )
)

term_group_stats = (
    with_parent
    .groupBy("term_group")
    .agg(F.avg("course_credits").alias("avg_course_credits"))
)

compared = (
    with_parent.alias("s")
    .join(term_group_stats.alias("g"), F.col("g.term_group") == F.col("s.term_group"), "inner")
    .filter(F.col("s.course_credits") > F.col("g.avg_course_credits"))
    .select(
        "s.*",
        "g.avg_course_credits",
        F.round(F.col("s.course_credits") - F.col("g.avg_course_credits"), 2).alias("course_credits_minus_avg"),
    )
)

result = (
    compared
    .groupingSets(
        [["course_credits", "student_id"], ["course_credits"], ["student_id"]],
        "course_credits", "student_id",
    )
    .agg(
        F.grouping("course_credits").alias("is_total_course_credits"),
        F.sum(F.when(~F.col("course_dept").isin("Art", "Math"), F.col("course_credits")).otherwise(0)).alias("course_credits_course_dept_not_in_art_math"),
        F.round(F.stddev("course_credits"), 2).alias("stddev_course_credits"),
    )
    .orderBy(
        F.col("course_credits").asc_nulls_last(),
        F.col("student_id").asc_nulls_last(),
    )
)
