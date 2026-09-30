from pyspark.sql import functions as F

filtered = df_enrollments.filter(F.col("course_id").isNotNull())

annotated = (
    filtered
    .select(
        "id",
        "student_id",
        "term",
        "grade_status",
        "enrolled_at",
        F.when(F.col("score") >= 73.0, "HIGH").when(F.col("score") >= 32.7, "MEDIUM").otherwise("LOW").alias("score_tier"),
        F.when(F.col("grade_status").isin("PASS", "INCOMPLETE"), "group_a").when(F.col("grade_status") == "WITHDRAWN", "group_b").otherwise("other").alias("grade_status_group"),
    )
)

enriched = (
    annotated
    .select(
        "id",
        "student_id",
        "term",
        "grade_status",
        "enrolled_at",
        "score_tier",
        "grade_status_group",
        F.length(F.trim(F.col("grade_status"))).alias("grade_status_len"),
        F.year(F.col("enrolled_at")).alias("enrolled_at_year"),
        F.length(F.trim(F.col("grade_status"))).alias("grade_status_len_2"),
    )
)

qualified = (
    enriched.alias("s")
    .join(df_students.alias("st"), (F.col("st.id") == F.col("s.student_id")) & (F.col("st.major").isin("Bio", "Biz", "Art")) & (~F.col("st.is_international")), "left_anti")
)

result = (
    qualified
    .groupBy("grade_status_len", "term")
    .pivot("grade_status_group", ["other", "group_b", "group_a"])
    .agg(F.max("enrolled_at_year").alias("max_v"), F.sum("enrolled_at_year").alias("sum_v"))
)
