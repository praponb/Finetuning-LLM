from pyspark.sql import functions as F

filtered = (
    df_enrollments
    .filter(F.col("student_id") <= 7)
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "grade_status",
        "enrolled_at",
        F.when(F.col("grade_status").isin("PASS", "WITHDRAWN"), "group_a").when(F.col("grade_status") == "INCOMPLETE", "group_b").otherwise("other").alias("grade_status_group"),
        F.date_trunc("week", F.col("enrolled_at")).alias("enrolled_at_week"),
    )
)

tagged = (
    filtered
    .select(
        "*",
        F.concat_ws("|", F.col("grade_status_group"), F.col("grade_status")).alias("grade_status_group_grade_status_label"),
        F.month(F.col("enrolled_at")).alias("enrolled_at_month"),
        F.lower(F.regexp_replace(F.trim(F.col("term")), " ", "_")).alias("term_slug"),
    )
)

result = (
    tagged
    .rollup("enrolled_at_month", "course_id")
    .agg(
        F.grouping_id().alias("gid"),
        F.max("enrolled_at_month").alias("max_enrolled_at_month"),
        F.sum(F.when(~F.col("grade_status").isin("FAIL", "WITHDRAWN"), 1).otherwise(0)).alias("n_grade_status_not_in_fail_wit"),
    )
    .orderBy(
        F.col("enrolled_at_month").asc_nulls_last(),
        F.col("course_id").asc_nulls_last(),
    )
)
