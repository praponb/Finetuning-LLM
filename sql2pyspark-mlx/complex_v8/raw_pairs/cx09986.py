from pyspark.sql import functions as F

prepared = (
    df_students
    .filter(F.col("gpa") > 2.0)
    .select(
        "id",
        "student_name",
        "cohort",
        "major",
        "enrolled_on",
        "gpa",
        "is_international",
        F.date_trunc("quarter", F.col("enrolled_on")).alias("enrolled_on_quarter"),
        F.month(F.col("enrolled_on")).alias("enrolled_on_month"),
        F.concat_ws("-", F.col("cohort"), F.col("student_name")).alias("cohort_student_name_label"),
    )
)

filtered = (
    prepared.alias("s")
    .join(df_enrollments.alias("e"), (F.col("e.student_id") == F.col("s.id")) & (F.col("e.grade_status") != "INCOMPLETE"), "left_semi")
)

labeled = (
    filtered
    .select(
        "*",
        F.round(F.col("enrolled_on_month") / F.nullif(F.col("gpa"), F.lit(0)), 3).alias("enrolled_on_month_per_gpa"),
        F.lower(F.regexp_replace(F.trim(F.col("student_name")), " ", "_")).alias("student_name_slug"),
        F.lower(F.trim(F.col("major"))).alias("major_clean"),
    )
)

result = (
    labeled
    .select("id", "enrolled_on_month_per_gpa", "gpa")
    .unpivot(
        ids=["id"],
        values=["enrolled_on_month_per_gpa", "gpa"],
        variableColumnName="measure",
        valueColumnName="metric_value",
    )
    .filter(F.col("metric_value").isNotNull())
    .orderBy("id", "measure")
)
