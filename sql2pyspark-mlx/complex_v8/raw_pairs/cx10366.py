from pyspark.sql import functions as F

valid = (
    df_enrollments
    .filter(F.col("term").isin("2024T2", "2024T1", "2025T1"))
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "grade_status",
        "enrolled_at",
    )
)

annotated = (
    valid
    .filter(F.col("course_id") >= 11)
    .select("*", F.length(F.trim(F.col("term"))).alias("term_len"))
)

derived = (
    annotated
    .select(
        "*",
        F.date_format(F.col("enrolled_at"), "MMM yyyy").alias("enrolled_at_label"),
        F.length(F.trim(F.col("term"))).alias("term_len_2"),
    )
)

result = (
    derived
    .select("id", "term_len_2", "term_len")
    .unpivot(
        ids=["id"],
        values=["term_len_2", "term_len"],
        variableColumnName="metric_name",
        valueColumnName="value",
    )
    .filter(F.col("value").isNotNull())
)
