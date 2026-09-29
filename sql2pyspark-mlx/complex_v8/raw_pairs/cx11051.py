from pyspark.sql import functions as F

prepared = (
    df_enrollments
    .filter(F.col("course_id") >= 7)
    .select(
        "id",
        "student_id",
        "course_id",
        "score",
        "grade_status",
        "enrolled_at",
        F.date_trunc("month", F.col("enrolled_at")).alias("enrolled_at_month"),
        F.concat(F.col("term"), F.lit("#"), F.col("id").cast("string")).alias("term_ref"),
        F.concat(F.col("term"), F.lit("#"), F.col("course_id").cast("string")).alias("term_ref_2"),
    )
)

summary = (
    prepared
    .groupBy(
        F.date_format("enrolled_at_month", "yyyy-MM").alias("enrolled_at_month_ym"),
        "course_id",
        "grade_status",
    )
    .agg(
        F.round(100 * F.count(F.when(F.col("grade_status").isNotNull(), 1)) / F.count("*"), 1).alias("pct_grade_status_is_not_null"),
        F.count("*").alias("row_count"),
        F.array_sort(F.collect_set("grade_status")).alias("grade_status_list"),
        F.max("enrolled_at").alias("last_enrolled_at"),
        F.round(F.sum("score"), 2).alias("total_score"),
    )
    .filter(F.col("row_count") >= 2)
)

flattened = (
    summary
    .select(
        "enrolled_at_month_ym",
        "course_id",
        "grade_status",
        "pct_grade_status_is_not_null",
        "row_count",
        "last_enrolled_at",
        "total_score",
        F.explode_outer("grade_status_list").alias("grade_status_list_item"),
    )
)

result = (
    flattened
    .select(
        "enrolled_at_month_ym",
        "course_id",
        "pct_grade_status_is_not_null",
        "total_score",
    )
    .unpivot(
        ids=["enrolled_at_month_ym", "course_id"],
        values=["pct_grade_status_is_not_null", "total_score"],
        variableColumnName="field",
        valueColumnName="amount",
    )
    .filter(F.col("amount").isNotNull())
)
