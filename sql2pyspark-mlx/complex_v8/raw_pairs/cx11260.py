from pyspark.sql import functions as F

recent_enrollments = (
    df_enrollments
    .filter(F.col("grade_status").isNotNull())
    .select(
        "id",
        "student_id",
        "course_id",
        "score",
        "grade_status",
        "enrolled_at",
        F.lower(F.regexp_replace(F.trim(F.col("grade_status")), " ", "_")).alias("grade_status_slug"),
        F.length(F.trim(F.col("grade_status"))).alias("grade_status_len"),
    )
)

per_group = (
    recent_enrollments
    .groupBy("student_id", "grade_status")
    .agg(
        F.countDistinct("id").alias("distinct_ids"),
        F.count(F.when(F.col("grade_status").isNotNull(), 1)).alias("n_grade_status_is_not_null"),
    )
)

joined = (
    per_group.alias("s")
    .join(df_students.alias("st"), F.col("st.id") == F.col("s.student_id"), "left")
    .select(
        "s.student_id",
        "s.grade_status",
        "s.n_grade_status_is_not_null",
        F.col("st.enrolled_on").alias("student_enrolled_on"),
        "st.student_name",
        F.col("st.major").alias("student_major"),
    )
)

result = (
    joined
    .groupingSets(
        [["n_grade_status_is_not_null", "grade_status", "student_major"], ["n_grade_status_is_not_null"], []],
        "n_grade_status_is_not_null", "grade_status", "student_major",
    )
    .agg(
        F.grouping_id().alias("gid"),
        F.max("student_enrolled_on").alias("last_student_enrolled_on"),
    )
    .orderBy(
        F.col("n_grade_status_is_not_null").asc_nulls_last(),
        F.col("grade_status").asc_nulls_last(),
        F.col("student_major").asc_nulls_last(),
    )
)
