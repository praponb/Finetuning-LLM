from pyspark.sql import functions as F

prepared = (
    df_courses
    .filter((F.col("credits") > 2) & (F.col("level").isNotNull()))
    .select(
        "id",
        "course_code",
        "dept",
        "credits",
        "level",
        F.coalesce(F.col("credits"), F.lit(0)).alias("credits_filled"),
    )
)

with_activity = (
    prepared.alias("s")
    .join(df_enrollments.alias("e"), (F.col("e.course_id") == F.col("s.id")) & (F.col("e.enrolled_at") >= F.lit("2025-01-25 11:00:00").cast("timestamp")), "left_semi")
)

result = (
    with_activity
    .select("id", "credits_filled", "credits")
    .unpivot(
        ids=["id"],
        values=["credits_filled", "credits"],
        variableColumnName="field",
        valueColumnName="value",
    )
    .filter(F.col("value").isNotNull())
)
