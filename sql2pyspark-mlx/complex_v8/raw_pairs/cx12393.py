from pyspark.sql import functions as F

scoped = (
    df_courses
    .filter(F.col("dept") != "Bio")
    .select(
        "id",
        "course_code",
        "dept",
        "credits",
        "level",
        F.lower(F.regexp_replace(F.trim(F.col("dept")), " ", "_")).alias("dept_slug"),
    )
)

labeled = (
    scoped
    .filter(F.lower(F.col("course_code")).like("%a1%"))
    .select(
        "*",
        F.abs(F.col("credits") - 2).alias("credits_distance"),
        F.length(F.trim(F.col("level"))).alias("level_len"),
        F.initcap(F.trim(F.col("level"))).alias("level_clean"),
    )
)

enrollment_stats = (
    df_enrollments
    .groupBy("course_id")
    .agg(
        F.count("*").alias("enrollment_count"),
        F.max("enrolled_at").alias("last_enrollment_enrolled_at"),
    )
)

with_counts = (
    labeled.alias("s")
    .join(enrollment_stats.alias("e"), F.col("e.course_id") == F.col("s.id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("e.enrollment_count"), F.lit(0)).alias("enrollment_count"),
        "e.last_enrollment_enrolled_at",
    )
)

result = (
    with_counts
    .select(
        "id",
        "level_len",
        "credits_distance",
        "enrollment_count",
    )
    .unpivot(
        ids=["id"],
        values=["level_len", "credits_distance", "enrollment_count"],
        variableColumnName="field",
        valueColumnName="reading",
    )
    .filter(F.col("reading").isNotNull())
)
