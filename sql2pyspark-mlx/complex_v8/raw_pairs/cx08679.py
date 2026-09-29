from pyspark.sql import functions as F

base_enrollments = (
    df_enrollments
    .filter(F.col("student_id") >= 16)
    .select(
        "id",
        "student_id",
        "course_id",
        "score",
        "grade_status",
        "enrolled_at",
        F.when(F.col("score") >= 58.8, "HIGH").when(F.col("score") >= 16.3, "MEDIUM").otherwise("LOW").alias("score_tier"),
        F.concat(F.col("grade_status"), F.lit("#"), F.col("id").cast("string")).alias("grade_status_ref"),
    )
)

enriched = (
    base_enrollments.alias("s")
    .join(df_students.alias("st"), F.col("st.id") == F.col("s.student_id"), "left")
    .select(
        "s.id",
        "s.student_id",
        "s.course_id",
        "s.score",
        "s.grade_status",
        "s.grade_status_ref",
        F.col("st.enrolled_on").alias("student_enrolled_on"),
    )
)

result = (
    enriched
    .groupingSets(
        [["grade_status", "course_id"], ["grade_status"], ["course_id"]],
        "grade_status", "course_id",
    )
    .agg(
        F.grouping_id().alias("gid"),
        F.array_sort(F.collect_set("grade_status")).alias("grade_status_list"),
    )
    .select(
        F.coalesce(F.col("grade_status"), F.lit("ALL")).alias("grade_status"),
        "course_id",
        "gid",
        "grade_status_list",
    )
    .orderBy(
        F.col("grade_status").asc_nulls_last(),
        F.col("course_id").asc_nulls_last(),
    )
)
