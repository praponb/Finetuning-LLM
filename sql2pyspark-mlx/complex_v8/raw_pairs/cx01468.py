from pyspark.sql import functions as F

scoped = (
    df_courses
    .filter(F.col("credits") <= 3)
    .select(
        "id",
        "course_code",
        "dept",
        "credits",
        "level",
    )
)

scored = (
    scoped
    .select(
        "id",
        "dept",
        "level",
        F.coalesce(F.col("credits"), F.lit(0)).alias("credits_filled"),
        F.abs(F.col("credits") - 2).alias("credits_distance"),
        F.lower(F.regexp_replace(F.trim(F.col("dept")), " ", "_")).alias("dept_slug"),
    )
)

without_matches = (
    scored.alias("s")
    .join(df_enrollments.alias("e"), F.col("e.course_id") == F.col("s.id"), "left_semi")
)

result = (
    without_matches
    .select(
        "id",
        "dept",
        "credits_filled",
        "credits_distance",
        "dept_slug",
    )
    .orderBy(F.col("dept").asc_nulls_last(), "dept_slug")
)
